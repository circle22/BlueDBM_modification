/*
The MIT License (MIT)

Copyright (c) 2014-2015 CSAIL, MIT

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

#include <stdio.h>
#include <stdint.h>
#include <errno.h>	/* strerr, errno */
#include <fcntl.h> /* O_RDWR */
#include <unistd.h> /* close */
#include <poll.h> /* poll */
#include <sys/mman.h> /* mmap */

#include "bdbm_drv.h"
#include "debug.h"
#include "platform.h"
#include "host_user.h"
#include "params.h"

#include "dm_proxy.h"
#include "dm_stub.h"

#include "utils/utime.h"
#include "utils/uthread.h"


/* interface for dm */
struct bdbm_dm_inf_t _bdbm_dm_inf = {
	.ptr_private = NULL,
	.probe = dm_proxy_probe,
	.open = dm_proxy_open,
	.close = dm_proxy_close,
	.make_req = dm_proxy_make_req,
	.end_req = dm_proxy_end_req,
	.load = dm_proxy_load,
	.store = dm_proxy_store,
};

typedef struct {
	/* nothing now */
	int fd;
	int stop;
	int punit;
	bdbm_thread_t* dm_proxy_thread;

	uint8_t* mmap_shared;
	uint8_t* ps; /* punit status */
	uint8_t** punit_main_pages;
	uint8_t** punit_oob_pages;

	bdbm_spinlock_t lock;
	struct bdbm_llm_req_t** llm_reqs;
} bdbm_dm_proxy_t;

int __dm_proxy_thread (void* arg);


uint32_t dm_proxy_probe (
	struct bdbm_drv_info* bdi, 
	struct nand_params* params)
{
	bdbm_dm_proxy_t* p = (bdbm_dm_proxy_t*)BDBM_DM_PRIV(bdi);
	int ret;

	if ((p->fd = open (BDBM_DM_IOCTL_DEVNAME, O_RDWR)) < 0) {
		bdbm_msg ("error: could not open a character device (re = %d)\n", p->fd);
		return 1;
	}

	if ((ret = ioctl (p->fd, BDBM_DM_IOCTL_PROBE, params)) != 0) {
		return 1;
	}

	p->punit = params->nr_chips_per_channel * params->nr_channels;

	bdbm_msg ("--------------------------------");
	bdbm_msg ("probe (): ret = %d", ret);
	bdbm_msg ("nr_channels: %u", (uint32_t)params->nr_channels);
	bdbm_msg ("nr_chips_per_channel: %u", (uint32_t)params->nr_chips_per_channel);
	bdbm_msg ("nr_blocks_per_chip: %u", (uint32_t)params->nr_blocks_per_chip);
	bdbm_msg ("nr_pages_per_block: %u", (uint32_t)params->nr_pages_per_block);
	bdbm_msg ("page_main_size: %u", (uint32_t)params->page_main_size);
	bdbm_msg ("page_oob_size: %u", (uint32_t)params->page_oob_size);
	bdbm_msg ("device_type: %u", (uint32_t)params->device_type);
	bdbm_msg ("# of punits: %u", (uint32_t)p->punit);
	bdbm_msg ("");

	return 0;
}

uint32_t dm_proxy_open (struct bdbm_drv_info* bdi)
{
	bdbm_dm_proxy_t* p = (bdbm_dm_proxy_t*)BDBM_DM_PRIV(bdi);
	uint64_t mmap_ofs = 0, loop;
	int size, ret;

	/* open bdbm_dm_stub */
	if ((ret = ioctl (p->fd, BDBM_DM_IOCTL_OPEN, &ret)) != 0) {
		bdbm_warning ("bdbm_dm_proxy_open () failed");
		return 1;
	}

	/* allocate space for llm_reqs */
	if ((p->llm_reqs = (struct bdbm_llm_req_t**)bdbm_malloc 
			(sizeof (struct bdbm_llm_req_t*) * p->punit)) == NULL) {
		bdbm_warning ("bdbm_malloc () failed");
		ioctl (p->fd, BDBM_DM_IOCTL_CLOSE, &ret);
		return 1;
	}

	size = 4096 + p->punit * 
		(bdi->ptr_bdbm_params->nand.page_main_size + 
		 bdi->ptr_bdbm_params->nand.page_oob_size);

	/* get mmap to check the status of a device */
	if ((p->mmap_shared = mmap (NULL, 
			size,
			PROT_READ | PROT_WRITE, 
			MAP_SHARED, 
			p->fd, 0)) == NULL) {
		bdbm_warning ("bdbm_dm_proxy_mmap () failed");
		ioctl (p->fd, BDBM_DM_IOCTL_CLOSE, &ret);
		return 1;
	}

	/* setup variables mapped to shared area */
	mmap_ofs = 0;
	p->ps = p->mmap_shared + mmap_ofs;
	mmap_ofs += 4096;
	p->punit_main_pages = (uint8_t**)bdbm_zmalloc (p->punit * sizeof (uint8_t*));
	p->punit_oob_pages = (uint8_t**)bdbm_zmalloc (p->punit * sizeof (uint8_t*));
	for (loop = 0; loop < p->punit; loop++) {
		p->punit_main_pages[loop] = p->mmap_shared + mmap_ofs;
		mmap_ofs += bdi->ptr_bdbm_params->nand.page_main_size;
		p->punit_oob_pages[loop] = p->mmap_shared + mmap_ofs;
		mmap_ofs += bdi->ptr_bdbm_params->nand.page_oob_size;
	}

	/* run a thread to monitor the status */
	if ((p->dm_proxy_thread = bdbm_thread_create (
			__dm_proxy_thread, bdi, "__dm_proxy_thread")) == NULL) {
		bdbm_warning ("bdbm_thread_create failed");
		ioctl (p->fd, BDBM_DM_IOCTL_CLOSE, &ret);
		return 1;
	}

	return 0;
}

void dm_proxy_close (
	struct bdbm_drv_info* bdi)
{
	bdbm_dm_proxy_t* p = (bdbm_dm_proxy_t*)BDBM_DM_PRIV(bdi);
	int ret;

	p->stop = 1;
	bdbm_thread_stop (p->dm_proxy_thread);

	ioctl (p->fd, BDBM_DM_IOCTL_CLOSE, &ret);
	close (p->fd);
}

uint32_t dm_proxy_make_req (
	struct bdbm_drv_info* bdi, 
	struct bdbm_llm_req_t* r)
{
	struct nand_params* np = (struct nand_params*)BDBM_GET_NAND_PARAMS(bdi);
	bdbm_dm_proxy_t* p = (bdbm_dm_proxy_t*)BDBM_DM_PRIV(bdi);
	bdbm_llm_req_ioctl_t ior;
	int loop, punit_id = GET_PUNIT_ID (bdi, r->phyaddr);
	int nr_kp_per_fp = 1;
	int ret;

	/* check error cases */
	bdbm_spin_lock (&p->lock);
	if (p->llm_reqs[punit_id] != NULL) {
		bdbm_warning ("a duplicate I/O request to the same punit is observed: p->llm_reqs[%d] = %p", 
			punit_id, p->llm_reqs[punit_id]);
		bdbm_spin_unlock (&p->lock);
		return 1;
	}
	p->llm_reqs[punit_id] = r;
	bdbm_spin_unlock (&p->lock);

	/* build llm_ioctl_req commands */
	ior.req_type = r->req_type;
	ior.lpa = r->lpa;
	ior.channel_no = r->phyaddr->channel_no;
	ior.chip_no = r->phyaddr->chip_no;
	ior.block_no = r->phyaddr->block_no;
	ior.page_no = r->phyaddr->page_no;
	ior.ret = r->ret;

	/* copy user-data to Kernel if it is necessary */
	if (r->req_type == REQTYPE_WRITE ||
		r->req_type == REQTYPE_RMW_WRITE ||
		r->req_type == REQTYPE_GC_WRITE) {
		for (loop = 0; loop < nr_kp_per_fp; loop++) {
			if (r->kpg_flags != NULL) 
				ior.kpg_flags[loop] = r->kpg_flags[loop];
			else
				ior.kpg_flags[loop] = 0; /* not used */
			memcpy (p->punit_main_pages[punit_id] + (loop * KERNEL_PAGE_SIZE), 
				r->pptr_kpgs[loop], KERNEL_PAGE_SIZE);
		}
		memcpy (p->punit_oob_pages[punit_id], 
			r->ptr_oob, bdi->ptr_bdbm_params->nand.page_oob_size);
	}

	/* send llm_ioctl_req to the device */
	if ((ret = ioctl (p->fd, BDBM_DM_IOCTL_MAKE_REQ, &ior)) != 0) {
		bdbm_warning ("bdbm_proxy_make_req () failed (ret = %d)\n", ret);
		return 1;
	}

	return 0;
}

int __dm_proxy_thread (void* arg) 
{
	struct bdbm_drv_info* bdi = (struct bdbm_drv_info*)arg;
	struct nand_params* np = (struct nand_params*)BDBM_GET_NAND_PARAMS(bdi);
	struct bdbm_dm_inf_t* dm_inf = (struct bdbm_dm_inf_t*)BDBM_GET_DM_INF(bdi);
	bdbm_dm_proxy_t* p = (bdbm_dm_proxy_t*)BDBM_DM_PRIV(bdi);
	int nr_kp_per_fp = 1;
	struct pollfd fds[1];
	int nr_punit;
	int loop;
	int ret;

	while (p->stop != 1) {
		/* prepare arguments for poll */
		fds[0].fd = p->fd;
		fds[0].events = POLLIN;

		/* call poll () with 3 seconds timout */
		ret = poll (fds, 1, 3000);	/* p->ps is shared by kernel, but it is only updated by kernel when poll () is called */

		/* (1) timeout: continue to check the device status */
		if (ret == 0)
			continue;

		/* (2) error: poll () returns error for some reasones */
		if (ret < 0) {
			bdbm_error ("poll () returns errors (ret: %d, msg: %s)", ret, strerror (errno));
			continue;
		}

		/* (3) success */
		if (ret > 0) {
			for (loop = 0; loop < p->punit; loop++) {
				if (p->ps[loop] == 1) {
					struct bdbm_llm_req_t* r = NULL;

					/* lookup the llm_reqs array to find a request finished */
					bdbm_spin_lock (&p->lock);
					if (p->llm_reqs[loop] == NULL) {
						bdbm_error ("CRITICAL: p->llm_reqs[loop] is NULL (punit_id: %d)", loop);
						bdbm_spin_unlock (&p->lock);
						continue;
					} 
					r = p->llm_reqs[loop];
					p->llm_reqs[loop] = NULL;
					bdbm_spin_unlock (&p->lock);

					/* copy Kernel-data to user-space if it is necessary */
					if (r->req_type == REQTYPE_READ ||
						r->req_type == REQTYPE_RMW_READ ||
						r->req_type == REQTYPE_GC_READ) {
						int k;
						for (k = 0; k < nr_kp_per_fp; k++) {
							memcpy (r->pptr_kpgs[k], p->punit_main_pages[loop] + (k * KERNEL_PAGE_SIZE), KERNEL_PAGE_SIZE);
						}
						memcpy (r->ptr_oob, p->punit_oob_pages[loop], bdi->ptr_bdbm_params->nand.page_oob_size);
					}

					/* call end_req () to end the request */
					dm_inf->end_req (bdi, r);

					/* hw is ready to accept a new request */
					p->ps[loop] = 0;
				}
			}
		}
	}

	pthread_exit (0);

	return 0;
}

void dm_proxy_end_req (
	struct bdbm_drv_info* bdi, 
	struct bdbm_llm_req_t* r)
{
	struct nand_params* np = (struct nand_params*)BDBM_GET_NAND_PARAMS(bdi);

	bdi->ptr_llm_inf->end_req (bdi, r);
}

uint32_t dm_proxy_load (
	struct bdbm_drv_info* bdi, 
	const char* fn)
{
	bdbm_warning ("dm_proxy_load is not implemented yet");
	return 0;
}

uint32_t dm_proxy_store (
	struct bdbm_drv_info* bdi, 
	const char* fn)
{
	bdbm_warning ("dm_proxy_store is not implemented yet");
	return 0;
}


/* functions exported to clients */
int bdbm_dm_init (struct bdbm_drv_info* bdi)
{
	bdbm_dm_proxy_t* p = NULL;

	if (_bdbm_dm_inf.ptr_private != NULL) {
		bdbm_warning ("_bdbm_dm_inf is already open");
		return 1;
	}

	/* create and initialize private variables */
	if ((p = (bdbm_dm_proxy_t*)bdbm_zmalloc 
			(sizeof (bdbm_dm_proxy_t))) == NULL) {
		bdbm_error ("bdbm_malloc failed");
		return 1;
	}
	p->fd = -1;
	p->ps = NULL;
	p->dm_proxy_thread = NULL;
	p->stop = 0;
	p->llm_reqs = NULL;
	bdbm_spin_lock_init (&p->lock);

	_bdbm_dm_inf.ptr_private = (void*)p;

	return 0;
}

struct bdbm_dm_inf_t* bdbm_dm_get_inf (struct bdbm_drv_info* bdi)
{
	return &_bdbm_dm_inf;
}

void bdbm_dm_exit (struct bdbm_drv_info* bdi) 
{
	bdbm_dm_proxy_t* p = (bdbm_dm_proxy_t*)BDBM_DM_PRIV(bdi);

	if (p == NULL) {
		bdbm_warning ("_bdbm_dm_inf is already closed");
		return;
	}
	bdbm_spin_lock_destory (&p->lock);
	bdbm_free (p);

	_bdbm_dm_inf.ptr_private = NULL;
}

