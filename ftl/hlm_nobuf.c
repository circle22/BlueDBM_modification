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
#include "uthash.h"

#include "algo/no_ftl.h"
#include "algo/block_ftl.h"
#include "algo/page_ftl.h"

#define BUFFERING_LLM_COUNT	(320)

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
	bdbm_llm_req_t* buffered_lr[BUFFERING_LLM_COUNT];
	uint64_t cur_buf_ofs;
	
	uint64_t cur_lr_idx;
	uint64_t flush_lr_idx;	
	uint64_t queuing_lr_count;

	uint64_t flush_threshold; // ch x bank
	uint64_t queuing_threshold; // ch x bank x 4

	// utilization
	uint64_t cumulative_check_count;
	uint64_t cumulative_pending_count;
	uint64_t utilization;
} bdbm_hlm_nobuf_private_t;

typedef struct {
	uint64_t id;
	uint32_t lr_idx;
	uint32_t buf_ofs;

	UT_hash_handle hh; /* makes this structure hashable */	
} bdbm_hlm_hash_entry;

bdbm_hlm_hash_entry* hash_info = NULL;
bdbm_hlm_hash_entry hash_entry[BUFFERING_LLM_COUNT*BDBM_MAX_PAGES]; // 320 x 8

void __hlm_nobuf_add_entry(bdbm_hlm_hash_entry* entry) {
	HASH_ADD_INT(hash_info, id, entry);
}

void __hlm_nobuf_delete_entry(bdbm_hlm_hash_entry* entry) {
	HASH_DEL(hash_info, entry);
}

bdbm_hlm_hash_entry* __hlm_nobuf_find_entry(int user_id) {
	bdbm_hlm_hash_entry* entry;

	HASH_FIND_INT(hash_info, &user_id, entry);
	return entry;
}

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

	for (i = 0; i < BUFFERING_LLM_COUNT; i++)
	{
		p->buffered_lr[i] = NULL;
	}
	
	p->cur_buf_ofs = 0;

	p->cur_lr_idx = 0;
	p->flush_lr_idx = 0;	
	p->queuing_lr_count = 0;

	p->flush_threshold = bdi->parm_dev.nr_channels * bdi->parm_dev.nr_chips_per_channel;
	p->queuing_threshold = BUFFERING_LLM_COUNT; // ch x bank x 4	
	
	// utilization
	p->cumulative_check_count = 0;
	p->cumulative_pending_count = 0;
	p->utilization = 0;

	bdbm_msg("creast threshold %lld, %lld", p->flush_threshold, p->queuing_threshold);
	/* keep the private structure */
	bdi->ptr_hlm_inf->ptr_private = (void*)p;
	_hlm_nobuf_inf.ptr_private = (void*)p;

	return 0;
}

void hlm_nobuf_destroy (bdbm_drv_info_t* bdi)
{
	bdbm_hlm_nobuf_private_t* p = (bdbm_hlm_nobuf_private_t*)(_hlm_nobuf_inf.ptr_private);

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
	bdbm_hlm_nobuf_private_t* p = (bdbm_hlm_nobuf_private_t*)(_hlm_nobuf_inf.ptr_private);
	bdbm_hlm_hash_entry* entry;

	entry = __hlm_nobuf_find_entry(lr->logaddr.lpa[lr->logaddr.ofs]);
	if (entry != NULL)
	{
		/*
		bdbm_msg("read hit : %lld", entry->id);
		bdbm_msg(" lr_idx: %lld", entry->lr_idx);
		bdbm_msg(" buf_ofs : %lld", entry->buf_ofs);
		bdbm_msg(" cur_idx : %lld", p->cur_lr_idx);
		bdbm_msg(" cur_off : %lld", p->cur_buf_ofs); */

		// read cache hit
		bdbm_bug_on (entry->id != lr->logaddr.lpa[lr->logaddr.ofs]);		
		bdbm_memcpy (lr->fmain.kp_ptr[lr->logaddr.ofs], p->buffered_lr[entry->lr_idx]->fmain.kp_ptr[entry->buf_ofs], KPAGE_SIZE);

		lr->fmain.kp_stt[lr->logaddr.ofs] = KP_STT_HOLE;
		lr->logaddr.lpa[lr->logaddr.ofs] = -1;

		return 0;		
	}
	else
	{
		return -1;
	}
}

int __hlm_flush_buffer(bdbm_drv_info_t* bdi)
{
	bdbm_hlm_nobuf_private_t* p = (bdbm_hlm_nobuf_private_t*)(_hlm_nobuf_inf.ptr_private);
	bdbm_ftl_inf_t* ftl = BDBM_GET_FTL_INF(bdi);
	int i, j;
	int llm_idx = p->flush_lr_idx;
	bdbm_llm_req_t* llm_req;
	bdbm_hlm_hash_entry * entry;

//	bdbm_msg("flush_buffer start: %lld, %lld, %lld - %lld", p->cur_lr_idx, p->flush_lr_idx, p->queuing_lr_count, p->utilization);

	for (i = 0; i < p->flush_threshold; i++)
	{
		llm_req = p->buffered_lr[llm_idx + i];
		llm_req->req_type = REQTYPE_WRITE;

		if (ftl->get_free_ppa (bdi, llm_req->logaddr.lpa[0], &llm_req->phyaddr) != 0)
		{
			bdbm_error ("`ftl->get_free_ppa' failed");
			bdbm_bug_on (1);
		}
		
		if (ftl->map_lpa_to_ppa (bdi, &(llm_req->logaddr), &(llm_req->phyaddr)) != 0) 
		{
			bdbm_error ("`ftl->map_lpa_to_ppa' failed");
			bdbm_bug_on (1);
		}

		/* send individual llm-reqs to llm */
		if (bdi->ptr_llm_inf->make_req (bdi, llm_req) != 0) {
			bdbm_error ("oops! make_req () failed");
			bdbm_bug_on (1);
		}

//		bdbm_msg(" _delete_entry %lld", llm_idx + i);

		// cache management.
		entry = hash_entry + ((llm_idx + i)<<3);
		for (j = 0; j < BDBM_MAX_PAGES; j++)
		{
			if (llm_req->logaddr.lpa[j] != -1)
			{
				__hlm_nobuf_delete_entry(entry + j);
			}
		}
	}

	p->flush_lr_idx += p->flush_threshold;
	if (p->flush_lr_idx == p->queuing_threshold)
	{
		p->flush_lr_idx = 0;
	}
	
	p->queuing_lr_count -= p->flush_threshold;

//	bdbm_msg("flush_buffer end: %lld, %lld, %lld", p->cur_lr_idx, p->flush_lr_idx, p->queuing_lr_count);
	return 0;
}



int32_t __hlm_buffered_write(bdbm_drv_info_t* bdi, bdbm_llm_req_t* lr)
{
	bdbm_hlm_nobuf_private_t* p = (bdbm_hlm_nobuf_private_t*)(_hlm_nobuf_inf.ptr_private);
	bdbm_ftl_inf_t* ftl = BDBM_GET_FTL_INF(bdi);
	int i;
	uint64_t registered = 0;
	bdbm_hlm_hash_entry* entry = NULL; 
	bdbm_llm_req_t* buffered_lr = NULL; 

	p->cumulative_check_count++;
	p->cumulative_pending_count += (p->queuing_lr_count);

	if (lr->logaddr.ofs != -1)
	{
		// cache hit management.	
		entry = __hlm_nobuf_find_entry(lr->logaddr.lpa[lr->logaddr.ofs]);
		if (entry != NULL)
		{
			/*	
			bdbm_msg("write hit : %lld", entry->id);
			bdbm_msg(" lr_idx: %lld", entry->lr_idx);
			bdbm_msg(" buf _ofs: %lld", entry->buf_ofs);*/
			// write cache hit
			bdbm_bug_on (entry->id != lr->logaddr.lpa[lr->logaddr.ofs]);		
			bdbm_memcpy(p->buffered_lr[entry->lr_idx]->fmain.kp_ptr[entry->buf_ofs], lr->fmain.kp_ptr[lr->logaddr.ofs], KPAGE_SIZE);
			
			lr->fmain.kp_stt[lr->logaddr.ofs] = KP_STT_HOLE;
			lr->logaddr.lpa[lr->logaddr.ofs] = -1;

			return 0;		
		}

		// 4KB write.
		if (p->cur_buf_ofs == 0)
		{
			p->buffered_lr[p->cur_lr_idx] = lr;
			registered = 1;
		}

		buffered_lr = p->buffered_lr[p->cur_lr_idx];
		buffered_lr->fmain.kp_stt[p->cur_buf_ofs] = KP_STT_DATA;
		bdbm_memcpy (buffered_lr->fmain.kp_ptr[p->cur_buf_ofs] , lr->fmain.kp_ptr[lr->logaddr.ofs], KPAGE_SIZE);

		buffered_lr->logaddr.lpa[p->cur_buf_ofs] = lr->logaddr.lpa[lr->logaddr.ofs];
		((int64_t*)buffered_lr->foob.data)[p->cur_buf_ofs] = lr->logaddr.lpa[lr->logaddr.ofs];		

		ftl->invalidate_lpa(bdi, lr->logaddr.lpa[lr->logaddr.ofs], 1);

		// cache hit management.
		entry = hash_entry + ((p->cur_lr_idx << 3) + p->cur_buf_ofs);
		entry->id = lr->logaddr.lpa[lr->logaddr.ofs];
		entry->lr_idx = p->cur_lr_idx;
		entry->buf_ofs = p->cur_buf_ofs;
		__hlm_nobuf_add_entry(entry);

		if (buffered_lr != lr)
		{
			lr->fmain.kp_stt[lr->logaddr.ofs] = KP_STT_HOLE;
			lr->logaddr.lpa[lr->logaddr.ofs] = -1;
		}

		lr->logaddr.ofs = p->cur_buf_ofs; // temp...
		p->cur_buf_ofs++;
	}
	else
	{
		// 32KB full write		
		if (p->cur_buf_ofs != 0)
		{
			buffered_lr = p->buffered_lr[p->cur_lr_idx];
			for (i = p->cur_buf_ofs; i < BDBM_MAX_PAGES; i++)
			{
				buffered_lr->logaddr.lpa[i] = -1; //workaround
			}
			
			p->cur_lr_idx++;
			if (p->cur_lr_idx == p->queuing_threshold)
			{
				p->cur_lr_idx = 0;
			}			
			p->queuing_lr_count++;
		}

		for (i = 0; i < BDBM_MAX_PAGES; i++)
		{
			// cache hit check
			entry = __hlm_nobuf_find_entry(lr->logaddr.lpa[i]);
			if (entry != NULL)
			{
				bdbm_bug_on (entry->id != lr->logaddr.lpa[i]);		
				bdbm_memcpy(p->buffered_lr[entry->lr_idx]->fmain.kp_ptr[entry->buf_ofs], lr->fmain.kp_ptr[i], KPAGE_SIZE);
				
				lr->fmain.kp_stt[i] = KP_STT_HOLE;
				lr->logaddr.lpa[i] = -1;

				continue;
			}

			((int64_t*)lr->foob.data)[i] = lr->logaddr.lpa[i];
			ftl->invalidate_lpa(bdi, lr->logaddr.lpa[i], 1);

			// cache hit management.
			bdbm_hlm_hash_entry* entry = hash_entry + ((p->cur_lr_idx << 3) + i);
			entry->id = lr->logaddr.lpa[i];
			entry->lr_idx = p->cur_lr_idx;
			entry->buf_ofs = i;
			__hlm_nobuf_add_entry(entry);
		}

		p->buffered_lr[p->cur_lr_idx] = lr;
		registered = 1;
		
		p->cur_buf_ofs = BDBM_MAX_PAGES;
	}

	if (p->cur_buf_ofs == BDBM_MAX_PAGES)
	{
		p->cur_buf_ofs = 0;
		p->cur_lr_idx++;
		if (p->cur_lr_idx == p->queuing_threshold)
		{
			p->cur_lr_idx = 0;
		}

		p->queuing_lr_count++;
	}

	if (registered == 1)
	{
		return -1;
	}
	else
	{
		return 0;
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

	bdbm_hlm_nobuf_private_t* p = (bdbm_hlm_nobuf_private_t*)(_hlm_nobuf_inf.ptr_private);

	if (p->queuing_lr_count >= p->flush_threshold)
	{
		//	depend on pending count.
		if ((bdi->ptr_llm_inf->get_queuing_count(bdi) < np->nr_chips_per_ssd) &&
			(ftl->is_gc_needed(bdi, 0) != ON_DEMAND_GC))
		{
			__hlm_flush_buffer(bdi);

			p->utilization = (p->cumulative_pending_count * 100)/(p->queuing_threshold * p->cumulative_check_count);
			if (p->cumulative_check_count > 2048)
			{
				p->cumulative_pending_count /= 2;
				p->cumulative_check_count/= 2; 
			}
		}
	}

	if (p->queuing_lr_count >= (p->queuing_threshold - hr->nr_llm_reqs - 1))
	{
		return 2;
	} 
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
			} 
			else if (bdbm_is_write (lr->req_type)) 
			{
//				bdbm_msg("write : %lld %lld, %lld", lr->logaddr.lpa[0], lr->logaddr.lpa[1], lr->logaddr.ofs) ;

				int32_t ret = __hlm_buffered_write(bdi, lr); 
				if (ret == -1)
				{
					lr->req_type = REQTYPE_FLUSH_WRITE;
					
					continue;
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
			if (bdbm_is_flush(lr->req_type))
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

void __hlm_nobuf_check_background_gc (bdbm_drv_info_t* bdi)
{
	bdbm_ftl_inf_t* ftl = (bdbm_ftl_inf_t*)BDBM_GET_FTL_INF(bdi);
	bdbm_hlm_nobuf_private_t* p = (bdbm_hlm_nobuf_private_t*)(_hlm_nobuf_inf.ptr_private);

	if (ftl->is_gc_needed (bdi, 0)) 
	{
		uint32_t ret = ftl->do_gc (bdi, p->utilization);
	}
}

void __hlm_nobuf_check_ondemand_gc (bdbm_drv_info_t* bdi, bdbm_hlm_req_t* hr)
{
	bdbm_ftl_params* dp = BDBM_GET_DRIVER_PARAMS (bdi);
	bdbm_ftl_inf_t* ftl = (bdbm_ftl_inf_t*)BDBM_GET_FTL_INF(bdi);
	bdbm_hlm_nobuf_private_t* p = (bdbm_hlm_nobuf_private_t*)(_hlm_nobuf_inf.ptr_private);

	if (dp->mapping_type == MAPPING_POLICY_PAGE) 
	{
		if ((bdbm_is_write(hr->req_type)) && (ftl->is_gc_needed(bdi, 0) == ON_DEMAND_GC)) 
		{
			uint32_t ret;	
		//	bdbm_msg ("[hlm_nobuf_make_req] forground GC start");
			do
			{
				ret = ftl->do_gc (bdi, p->utilization);
			}
			while((ftl->is_gc_needed (bdi, 0) == ON_DEMAND_GC) || (ret != 0));
		//	bdbm_msg ("[hlm_nobuf_make_req] forground GC finish %ld", ret);
		}
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

	/* perform i/o */
	if (bdbm_is_trim (hr->req_type)) {
		if ((ret = __hlm_nobuf_make_trim_req (bdi, hr)) == 0) {
			/* call 'ptr_host_inf->end_req' directly */
			bdi->ptr_host_inf->end_req (bdi, hr);
			/* hr is now NULL */
		}
	} else {
		/* do we need to do garbage collection? */
		while ((ret = __hlm_nobuf_make_rw_req (bdi, hr)) == 2)
		{
			__hlm_nobuf_check_ondemand_gc (bdi, hr);
		}
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


uint32_t hlm_nobuf_get_utilization(bdbm_drv_info_t* bdi)
{
	bdbm_hlm_nobuf_private_t* p = (bdbm_hlm_nobuf_private_t*)(_hlm_nobuf_inf.ptr_private);

	return p->utilization;
}

uint32_t hlm_nobuf_flush_buffer(bdbm_drv_info_t* bdi)
{
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS(bdi);
	bdbm_ftl_inf_t* ftl = BDBM_GET_FTL_INF(bdi);
	bdbm_hlm_nobuf_private_t* p = (bdbm_hlm_nobuf_private_t*)(_hlm_nobuf_inf.ptr_private);

	if (p->queuing_lr_count >= p->flush_threshold)
	{
		//	depend on pending count.
		if ((bdi->ptr_llm_inf->get_queuing_count(bdi) < np->nr_chips_per_ssd) &&
			(ftl->is_gc_needed(bdi, 0) != ON_DEMAND_GC))
		{
			__hlm_flush_buffer(bdi);

			p->utilization = (p->queuing_lr_count * 100)/p->queuing_threshold;
		}

		return 1;
	}
	else
	{
		return 0;
	}
}

