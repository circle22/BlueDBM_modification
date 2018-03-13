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

#if defined(KERNEL_MODE)
#include <linux/module.h>
#include <linux/blkdev.h>

#elif defined(USER_MODE)
#include <stdio.h>
#include <stdint.h>

#else
#error Invalid Platform (KERNEL_MODE or USER_MODE)
#endif

#include "debug.h"
#include "params.h"
#include "bdbm_drv.h"
#include "hlm_nobuf.h"
#include "hlm_reqs_pool.h"
#include "utime.h"
#include "umemory.h"

#include "algo/no_ftl.h"
#include "algo/block_ftl.h"
#include "algo/page_ftl.h"


/* interface for hlm_nobuf */
bdbm_hlm_inf_t _hlm_nobuf_inf = {
	.ptr_private = NULL,
	.create = hlm_nobuf_create,
	.destroy = hlm_nobuf_destroy,
	.make_req = hlm_nobuf_make_req,
	.end_req = hlm_nobuf_end_req,
	/*.load = hlm_nobuf_load,*/
	/*.store = hlm_nobuf_store,*/
};

/* data structures for hlm_nobuf */
typedef struct {
	bdbm_llm_req_t* buffered_lr;
    uint8_t* buf[BDBM_MAX_PAGES];
    uint64_t oob[BDBM_MAX_PAGES];
    uint8_t cur_buf_ofs;
} bdbm_hlm_nobuf_private_t;


/* functions for hlm_nobuf */
uint32_t hlm_nobuf_create (bdbm_drv_info_t* bdi)
{
	bdbm_hlm_nobuf_private_t* p;
    int i = 0;

	/* create private */
	if ((p = (bdbm_hlm_nobuf_private_t*)bdbm_malloc
			(sizeof(bdbm_hlm_nobuf_private_t))) == NULL) {
		bdbm_error ("bdbm_malloc failed");
		return 1;
	}

	for (i = 0; i < BDBM_MAX_PAGES; i++){
		p->buf[i] = (uint8_t*)bdbm_malloc(KPAGE_SIZE);
		p->oob[i] = -1;
    }
	p->buffered_lr = NULL;
	p->cur_buf_ofs = 0;

	/* keep the private structure */
	bdi->ptr_hlm_inf->ptr_private = (void*)p;

	return 0;
}

void hlm_nobuf_destroy (bdbm_drv_info_t* bdi)
{
	bdbm_hlm_nobuf_private_t* p = (bdbm_hlm_nobuf_private_t*)BDBM_HLM_PRIV(bdi);

	/* free priv */
	bdbm_free (p);
}

uint32_t __hlm_nobuf_make_trim_req (bdbm_drv_info_t* bdi, bdbm_hlm_req_t* ptr_hlm_req)
{
	bdbm_ftl_inf_t* ftl = (bdbm_ftl_inf_t*)BDBM_GET_FTL_INF(bdi);
	uint64_t i;

	for (i = 0; i < ptr_hlm_req->len; i++) {
		ftl->invalidate_lpa (bdi, ptr_hlm_req->lpa + i, 1);
	}

	return 0;
}

void __print_buffer(uint8_t* ptr)
{
	uint64_t* tmp = (uint64_t*)ptr;
	uint64_t index;

	for (index = 0; index < 64; index++)
	{
		bdbm_msg("%llx,%llx,%llx,%llx,%llx,%llx,%llx,%llx", tmp[index*8],  tmp[index*8 + 1], tmp[index*8 + 2], tmp[index*8 + 3], tmp[index*8 + 4], tmp[index*8 + 5], tmp[index*8 + 6], tmp[index*8 + 7]);
	}
	bdbm_msg("-----");
}


uint32_t __hlm_buffered_read(bdbm_drv_info_t* bdi, bdbm_llm_req_t* lr){
	bdbm_hlm_nobuf_private_t* p = (bdbm_hlm_nobuf_private_t*)BDBM_HLM_PRIV(bdi);
	int i = 0;

	for (i = 0; i < p->cur_buf_ofs; i++){
		if (p->buffered_lr->logaddr.lpa[i] == lr->logaddr.lpa[lr->logaddr.ofs])
		{
			bdbm_memcpy (lr->fmain.kp_ptr[lr->logaddr.ofs], p->buffered_lr->fmain.kp_ptr[i], KPAGE_SIZE);
			lr->fmain.kp_stt[lr->logaddr.ofs] = KP_STT_HOLE;
			lr->logaddr.lpa[lr->logaddr.ofs] = -1;

			return i;
		}
	}

    return -1;
}

int __hlm_flush_buffer(bdbm_drv_info_t* bdi, bdbm_llm_req_t* lr){
	bdbm_hlm_nobuf_private_t* p = (bdbm_hlm_nobuf_private_t*)BDBM_HLM_PRIV(bdi);
	bdbm_ftl_inf_t* ftl = BDBM_GET_FTL_INF(bdi);
	int i = 0, cnt = 0;

	lr->logaddr.ofs = -1;
	for(i = 0; i < p->cur_buf_ofs; i++){
		lr->fmain.kp_ptr[i] = lr->fmain.kp_pad[i];
		bdbm_memcpy (lr->fmain.kp_ptr[i], p->buf[i], KPAGE_SIZE);
		lr->fmain.kp_stt[i] = KP_STT_DATA;
		lr->logaddr.lpa[i] = p->oob[i];
		p->oob[i] = -1;
		cnt++;
	}
	p->cur_buf_ofs = 0;

	return cnt;
}



uint32_t __hlm_buffered_write(bdbm_drv_info_t* bdi, bdbm_llm_req_t* lr){
	bdbm_hlm_nobuf_private_t* p = (bdbm_hlm_nobuf_private_t*)BDBM_HLM_PRIV(bdi);
	bdbm_ftl_inf_t* ftl = BDBM_GET_FTL_INF(bdi);
	int i = 0, same = -1;
	uint64_t registered = 0;

	if (p->cur_buf_ofs == 0)
	{
		p->buffered_lr = lr;
		registered = 1;
	}
	else
	{
		for(i = 0; i < p->cur_buf_ofs; i++){
			if(p->buffered_lr->logaddr.lpa[i] == lr->logaddr.lpa[lr->logaddr.ofs])
				same = i;
		}
	}

	if (same == -1)
	{
		p->buffered_lr->fmain.kp_ptr[p->cur_buf_ofs] = lr->fmain.kp_ptr[lr->logaddr.ofs];
		p->buffered_lr->logaddr.lpa[p->cur_buf_ofs] = lr->logaddr.lpa[lr->logaddr.ofs];
		((int64_t*)p->buffered_lr->foob.data)[p->cur_buf_ofs] = lr->logaddr.lpa[lr->logaddr.ofs];		
		p->buffered_lr->fmain.kp_stt[p->cur_buf_ofs] = KP_STT_DATA;

		ftl->invalidate_lpa(bdi, lr->logaddr.lpa[lr->logaddr.ofs], 1);
		p->cur_buf_ofs++;
	}
	else
	{
		p->buffered_lr->fmain.kp_ptr[same] = lr->fmain.kp_ptr[lr->logaddr.ofs];
	}

	if (p->buffered_lr != lr)
	{
		lr->fmain.kp_stt[lr->logaddr.ofs] = KP_STT_HOLE;
		lr->logaddr.lpa[lr->logaddr.ofs] = -1;
	}

	if (registered == 1)
	{
		return 0;
	}
	else
	{
		return p->cur_buf_ofs;
	}
}

void _display_hex_values (uint8_t* host)
{
	bdbm_msg (" * HOST: %x %x %x %x %x", host[0], host[1], host[2], host[3], host[4]);
}

uint32_t __hlm_nobuf_make_rw_req (bdbm_drv_info_t* bdi, bdbm_hlm_req_t* hr)
{
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS(bdi);
	bdbm_ftl_inf_t* ftl = BDBM_GET_FTL_INF(bdi);
	bdbm_llm_req_t* lr = NULL;
	uint64_t i = 0, j = 0, sp_ofs;

	bdbm_hlm_nobuf_private_t* p = (bdbm_hlm_nobuf_private_t*)BDBM_HLM_PRIV(bdi);	
	uint64_t registered_idx = 0xFF;

	/* perform mapping with the FTL */
	bdbm_hlm_for_each_llm_req (lr, hr, i) {
		/* (1) get the physical locations through the FTL */
		if (bdbm_is_normal (lr->req_type)) {
			/* handling normal I/O operations */
			if (bdbm_is_read (lr->req_type)) {
				if(__hlm_buffered_read(bdi, lr) == -1){
					if (ftl->get_ppa (bdi, lr->logaddr.lpa[0], &lr->phyaddr, &sp_ofs) != 0) {
						/* Note that there could be dummy reads (e.g., when the
						 * file-systems are initialized) */
						lr->req_type = REQTYPE_READ_DUMMY;
					//	bdbm_msg("dummy read");
					} else {
						hlm_reqs_pool_relocate_kp (lr, sp_ofs);
//						bdbm_msg("normal read : %lld", lr->logaddr.lpa[0]);
					}
				}
			} else if (bdbm_is_write (lr->req_type)) {
				if(lr->logaddr.ofs != -1)
				{
					uint32_t ret = __hlm_buffered_write(bdi, lr); 
					if (ret == 0)
					{
						registered_idx = i;
					}
					else if(ret == BDBM_MAX_PAGES){
						//bdbm_msg("partial write - flush");
						if (ftl->get_free_ppa (bdi, p->buffered_lr->logaddr.lpa[0], &p->buffered_lr->phyaddr) != 0)
						{
							bdbm_error ("`ftl->get_free_ppa' failed");
							goto fail;
						}

						if (ftl->map_lpa_to_ppa (bdi, &(p->buffered_lr->logaddr), &(p->buffered_lr->phyaddr)) != 0) 
						{
							bdbm_error ("`ftl->map_lpa_to_ppa' failed");
							goto fail;
						}

						/* send individual llm-reqs to llm */
						if (bdi->ptr_llm_inf->make_req (bdi, p->buffered_lr) != 0) {
							bdbm_error ("oops! make_req () failed");
							bdbm_bug_on (1);
						}
						
						p->buffered_lr = NULL;
						p->cur_buf_ofs = 0;
					}
				}
                else{
					//bdbm_msg("32KB write"); 
					if (ftl->get_free_ppa (bdi, lr->logaddr.lpa[0], &lr->phyaddr) != 0) {
                        bdbm_error ("`ftl->get_free_ppa' failed");
                        goto fail;
                    }
                    if (ftl->map_lpa_to_ppa (bdi, &lr->logaddr, &lr->phyaddr) != 0) {
                        bdbm_error ("`ftl->map_lpa_to_ppa' failed");
                        goto fail;
                    }
                }
			} 
			else {
				bdbm_error ("oops! invalid type (%x)", lr->req_type);
				bdbm_bug_on (1);
			}
		} 
		else if (bdbm_is_rmw (lr->req_type)) {
			bdbm_phyaddr_t* phyaddr = &lr->phyaddr_src;

			/* finding the location of the previous data */ 
			if (ftl->get_ppa (bdi, lr->logaddr.lpa[0], phyaddr, &sp_ofs) != 0) {
				/* if it was not written before, change it to a write request */
				lr->req_type = REQTYPE_WRITE;
				phyaddr = &lr->phyaddr;
			} else {
				hlm_reqs_pool_relocate_kp (lr, sp_ofs);
				phyaddr = &lr->phyaddr_dst;
			}

			/* getting the location to which data will be written */

			if (ftl->get_free_ppa (bdi, lr->logaddr.lpa[0], phyaddr) != 0) {
				bdbm_error ("`ftl->get_free_ppa' failed");
				goto fail;
			}

			if (ftl->map_lpa_to_ppa (bdi, &lr->logaddr, phyaddr) != 0) {
					bdbm_error ("`ftl->map_lpa_to_ppa' failed");
					goto fail;
				}
			} else {
				bdbm_error ("oops! invalid type (%x)", lr->req_type);
				bdbm_bug_on (1);
		}

		/* (2) setup oob */
		for (j = 0; j < BDBM_MAX_PAGES; j++) {
			((int64_t*)lr->foob.data)[j] = lr->logaddr.lpa[j];
		}
	}
	
	/* (3) send llm_req to llm */
	if (bdi->ptr_llm_inf->make_reqs == NULL) {
		/* send individual llm-reqs to llm */
		bdbm_hlm_for_each_llm_req (lr, hr, i) {
			if (i == registered_idx)
			{
				// this llm request is used for buffering
				continue;
			}

			if (bdi->ptr_llm_inf->make_req (bdi, lr) != 0) {
				bdbm_error ("oops! make_req () failed");
				bdbm_bug_on (1);
			}
		}
	} else {
		/* send a bulk of llm-reqs to llm if make_reqs is supported */
		if (bdi->ptr_llm_inf->make_reqs (bdi, hr) != 0) {
			bdbm_error ("oops! make_reqs () failed");
			bdbm_bug_on (1);
		}
	}

	bdbm_bug_on (hr->nr_llm_reqs != i);
	
	return 0;

fail:
	return 1;
}

/* TODO: it must be more general... */
void __hlm_nobuf_check_ondemand_gc (bdbm_drv_info_t* bdi, bdbm_hlm_req_t* hr)
{
	bdbm_ftl_params* dp = BDBM_GET_DRIVER_PARAMS (bdi);
	bdbm_ftl_inf_t* ftl = (bdbm_ftl_inf_t*)BDBM_GET_FTL_INF(bdi);

	if (dp->mapping_type == MAPPING_POLICY_PAGE) {
		uint32_t loop;

#ifndef DWHONG
		/* see if foreground GC is needed or not */
		for (loop = 0; loop < 10; loop++) {
			if (bdbm_is_write(hr->req_type) && 
				ftl->is_gc_needed != NULL && 
				ftl->is_gc_needed (bdi, 0)) {
				/* perform GC before sending requests */ 
				//bdbm_msg ("[hlm_nobuf_make_req] trigger GC");
				ftl->do_gc (bdi, 0);
			} else
				break;
		}
		asdfasdf
#else
		if (bdbm_is_write(hr->req_type) && 
			ftl->is_gc_needed != NULL && ftl->is_gc_needed (bdi, 0)) 
		{
			uint32_t ret;	
			//bdbm_msg ("[hlm_nobuf_make_req] forground GC start");
			do
			{
				ret = ftl->do_gc (bdi, 0);
			}
			while(ftl->is_gc_needed (bdi, 0) || (ret != 0));
			
			//bdbm_msg ("[hlm_nobuf_make_req] forground GC finish %ld", ret);
		}
#endif
	} else if (dp->mapping_type == MAPPING_POLICY_RSD ||
			   dp->mapping_type == MAPPING_POLICY_BLOCK) {
		/* perform mapping with the FTL */
		if (hr->req_type == REQTYPE_WRITE && ftl->is_gc_needed != NULL) {
			bdbm_llm_req_t* lr = NULL;
			uint64_t i = 0;
			bdbm_hlm_for_each_llm_req (lr, hr, i) {
				/* NOTE: segment-level ftl does not support fine-grain rmw */
				if (ftl->is_gc_needed (bdi, lr->logaddr.lpa[0])) {
					/* perform GC before sending requests */ 
					//bdbm_msg ("[hlm_nobuf_make_req] trigger GC");
					ftl->do_gc (bdi, lr->logaddr.lpa[0]);
				}
			}
		}
	} else {
		/* do nothing */
	}
}

uint32_t hlm_nobuf_make_req (bdbm_drv_info_t* bdi, bdbm_hlm_req_t* hr)
{
	uint32_t ret;
	bdbm_stopwatch_t sw;
	bdbm_stopwatch_start (&sw);

	/* is req_type correct? */
//	bdbm_bug_on (!bdbm_is_normal (hr->req_type));

#if 0
	/* trigger gc if necessary */
	if (dp->mapping_type != MAPPING_POLICY_DFTL) {
		bdbm_ftl_inf_t* ftl = (bdbm_ftl_inf_t*)BDBM_GET_FTL_INF(bdi);
		/* see if foreground GC is needed or not */
		for (loop = 0; loop < 10; loop++) {
			if (hr->req_type == REQTYPE_WRITE && 
				ftl->is_gc_needed != NULL && 
				ftl->is_gc_needed (bdi, 0)) {
				/* perform GC before sending requests */ 
				bdbm_msg ("[hlm_nobuf_make_req] trigger GC");
				ftl->do_gc (bdi, 0);
			} else
				break;
		}
	}
#endif

	/* perform i/o */
	if (bdbm_is_trim (hr->req_type)) {
		if ((ret = __hlm_nobuf_make_trim_req (bdi, hr)) == 0) {
			/* call 'ptr_host_inf->end_req' directly */
			bdi->ptr_host_inf->end_req (bdi, hr);
			/* hr is now NULL */
		}
	} else {
		/* do we need to do garbage collection? */
		__hlm_nobuf_check_ondemand_gc (bdi, hr);

		ret = __hlm_nobuf_make_rw_req (bdi, hr);
	} 

	return ret;
}

void __hlm_nobuf_end_blkio_req (bdbm_drv_info_t* bdi, bdbm_llm_req_t* lr)
{
	bdbm_hlm_req_t* hr = (bdbm_hlm_req_t* )lr->ptr_hlm_req;

	/* increase # of reqs finished */
	atomic64_inc (&hr->nr_llm_reqs_done);
	lr->req_type |= REQTYPE_DONE;

	if (atomic64_read (&hr->nr_llm_reqs_done) == hr->nr_llm_reqs) {
		/* finish the host request */
		bdi->ptr_host_inf->end_req (bdi, hr);
	}
}

void __hlm_nobuf_end_gcio_req (bdbm_drv_info_t* bdi, bdbm_llm_req_t* lr)
{
	bdbm_hlm_req_gc_t* hr_gc = (bdbm_hlm_req_gc_t* )lr->ptr_hlm_req;

	atomic64_inc (&hr_gc->nr_llm_reqs_done);
	lr->req_type |= REQTYPE_DONE;

/*	will not use semaphore.
	if (atomic64_read (&hr_gc->nr_llm_reqs_done) == hr_gc->nr_llm_reqs) {
		bdbm_sema_unlock (&hr_gc->done);
	}
*/	
}

void hlm_nobuf_end_req (bdbm_drv_info_t* bdi, bdbm_llm_req_t* lr)
{
	if (bdbm_is_gc (lr->req_type)) {
		__hlm_nobuf_end_gcio_req (bdi, lr);
	}
	else {
		__hlm_nobuf_end_blkio_req (bdi, lr);
	}
}

