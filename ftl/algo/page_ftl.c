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

#if defined (KERNEL_MODE)
#include <linux/module.h>
#include <linux/slab.h>
#include <linux/log2.h>
#include <linux/random.h>

#elif defined (USER_MODE)
#include <stdio.h>
#include <stdint.h>
#include "uilog.h"
#include "upage.h"

#else
#error Invalid Platform (KERNEL_MODE or USER_MODE)
#endif

#include "bdbm_drv.h"
#include "params.h"
#include "debug.h"
#include "utime.h"
#include "ufile.h"
#include "umemory.h"
#include "hlm_reqs_pool.h"

#include "algo/abm.h"
#include "algo/page_ftl.h"

#ifdef COPYBACK_QUOTA
	#ifdef PER_PAGE_COPYBACK_MANAGEMENT
		#define PER_PAGE_QUOTA
	#else
		#define PER_BLOCK_QUOTA
	#endif
#endif

/* FTL interface */
bdbm_ftl_inf_t _ftl_page_ftl = {
	.ptr_private = NULL,
	.create = bdbm_page_ftl_create,
	.destroy = bdbm_page_ftl_destroy,
	.get_free_ppa = bdbm_page_ftl_get_free_ppa,
	.get_ppa = bdbm_page_ftl_get_ppa,
	.map_lpa_to_ppa = bdbm_page_ftl_map_lpa_to_ppa,
	.invalidate_lpa = bdbm_page_ftl_invalidate_lpa,
	.do_gc = bdbm_page_ftl_do_gc,
	.is_gc_needed = bdbm_page_ftl_is_gc_needed,
	.scan_badblocks = bdbm_page_badblock_scan,

	.get_token = bdbm_page_ftl_get_token,
	.consume_token = bdbm_page_ftl_consume_token,
	.flush_meta = bdbm_page_ftl_flush_meta,
	/*.load = bdbm_page_ftl_load,*/
	/*.store = bdbm_page_ftl_store,*/
	/*.get_segno = NULL,*/
};


/* data structures for block-level FTL */
enum BDBM_PFTL_PAGE_STATUS {
	PFTL_PAGE_NOT_ALLOCATED = 0,
	PFTL_PAGE_VALID,
	PFTL_PAGE_INVALID,
	PFTL_PAGE_INVALID_ADDR = -1ULL,
};

typedef struct {
	uint8_t status; /* BDBM_PFTL_PAGE_STATUS */
	bdbm_phyaddr_t phyaddr; /* physical location */
	uint8_t sp_off;
} bdbm_page_mapping_entry_t;

typedef struct {
	uint32_t threshold_copyback;
	uint32_t proportion;
} blk_distribution_info; 

#ifdef COPYBACK_QUOTA
typedef struct {	
	uint32_t threshold_copyback;
	uint32_t diminishing_quota;
#ifdef PER_BLOCK_QUOTA	
	uint32_t copyback_quota;			
#endif
} quota_info;
#endif

typedef struct {
	bdbm_abm_info_t* bai;
	bdbm_page_mapping_entry_t* ptr_mapping_table;
	char* panMoveCount;
	bdbm_spinlock_t ftl_lock;
	uint64_t nr_punits;
	uint64_t nr_punits_pages;

	/* for the management of active blocks */
	uint64_t curr_puid;
	uint64_t curr_page_ofs;
	bdbm_abm_block_t** ac_bab;

	/* reserved for gc (reused whenever gc is invoked) */
	bdbm_abm_block_t** gc_src_bab;
	bdbm_abm_block_t** gc_dst_bab[MAX_COPY_BACK];

	uint64_t* gc_src_blk_offs[PLANE_NUMBER]; // for each ch x way.
	uint64_t* gc_dst_blk_offs[MAX_COPY_BACK]; // for each ch x way x copybackCount	

	bdbm_hlm_req_gc_t gc_hlm;
	bdbm_hlm_req_gc_t gc_hlm_w;

	/* for bad-block scanning */
	bdbm_sema_t badblk;

	// infomative data.	
	uint64_t host_write_count[64];
	uint64_t total_write_count[64];
	uint64_t invald_page_count[64];
	uint64_t block_info[MAX_COPY_BACK+1];

	uint64_t src_valid;
	uint64_t src_valid_page_count;
	uint64_t src_unit_hand;
	uint64_t src_plane_hand;	
	uint64_t src_unit_idx[PLANE_NUMBER][64];
	uint64_t src_plane_idx[PLANE_NUMBER][64];	

	uint64_t partial_read_start;
	uint64_t partial_read_end;
	uint64_t partial_head;
	uint64_t partial_tail;
	uint64_t required_subpage_count;
	uint64_t buffered_subpage_count;

	uint64_t erase_idx_start;
	uint64_t host_meta_idx_start;
	uint64_t gc_meta_idx_start;
	uint64_t meta_load_idx_start;
	uint8_t* cached_copyback_info;

#ifdef COPYBACK_QUOTA
	uint32_t initial_quota;
	uint32_t new_quota; // used for per page quota.

	blk_distribution_info distribution_info[10];

	uint32_t quota_to_dstIdx[13];
	uint32_t dstIdx_to_quota[13];

	quota_info* blk_quota_data;
#endif


	uint64_t dst_offset;	
	uint64_t dst_index;	

	uint64_t dma_write;
	uint64_t bypass_write;

	uint64_t external_die_difference_count;	
	uint64_t external_partial_valid_count;	
	uint64_t external_refresh_count;	
	uint64_t internal_copy_count;
	
	uint64_t gc_subpages_move_unit;
	
	uint64_t gc_copy_count;
	uint64_t host_update_count;
	uint64_t nop_count;

	uint64_t gc_count;
	uint64_t utilization;
	uint64_t gc_mode;
	uint64_t alloc_idx;

	// flow ctrl
	uint32_t token_mode; // on / off
	uint32_t token_count; // valid only when token mode is on.
	uint32_t generated_token;
} bdbm_page_ftl_private_t;


bdbm_page_mapping_entry_t* __bdbm_page_ftl_create_mapping_table (
	bdbm_device_params_t* np)
{
	bdbm_page_mapping_entry_t* me;
	uint64_t loop;

	/* create a page-level mapping table */
	if ((me = (bdbm_page_mapping_entry_t*)bdbm_zmalloc 
			(sizeof (bdbm_page_mapping_entry_t) * np->nr_subpages_per_ssd)) == NULL) {
		return NULL;
	}

	/* initialize a page-level mapping table */
	for (loop = 0; loop < np->nr_subpages_per_ssd; loop++) {
		me[loop].status = PFTL_PAGE_NOT_ALLOCATED;
		me[loop].phyaddr.channel_no = PFTL_PAGE_INVALID_ADDR;
		me[loop].phyaddr.way_no = PFTL_PAGE_INVALID_ADDR;
		me[loop].phyaddr.block_no = PFTL_PAGE_INVALID_ADDR;
		me[loop].phyaddr.page_no = PFTL_PAGE_INVALID_ADDR;
		me[loop].sp_off = -1;
	}

	/* return a set of mapping entries */
	return me;
}


void __bdbm_page_ftl_destroy_mapping_table (
	bdbm_page_mapping_entry_t* me)
{
	if (me == NULL)
		return;
	bdbm_free (me);
}

uint32_t __bdbm_page_ftl_get_active_blocks (
	bdbm_device_params_t* np,
	bdbm_abm_info_t* bai,
	bdbm_abm_block_t** bab)
{
	uint64_t i, j;

	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;

	for (i = 0; i < np->nr_channels; i++) {
		for (j = 0; j < np->nr_units_per_channel; j++) {
			if (bai->anr_free_blks[i][j] < 2)
			{
				// Need to go GC.
				return 1;
			}
		}
	}

	/* get a set of free blocks for active blocks */
	for (i = 0; i < np->nr_channels; i++) {
		for (j = 0; j < np->nr_units_per_channel; j++) 
		{
			uint64_t plane;			
			/* restore previous active block */
			if ((*bab) != NULL)
			{
				for (plane = 0; plane < np->nr_planes; plane++)
				{
					bdbm_abm_make_dirty_blk(bai, i, j/np->nr_groups_per_die, (*bab)->block_no + plane);
				}
			}
			
			/* prepare & commit free blocks */
			for (plane = 0; plane < np->nr_planes; plane++)
			{
				bdbm_abm_block_t* blk;
				
				if ((blk = bdbm_abm_get_free_block_prepare (bai, i, j))) {
					bdbm_abm_get_free_block_commit (bai, blk);
					/*bdbm_msg ("active blk = %p", *bab);*/
					
					blk->info = 10;// active
					blk->copy_count = 0;
					p->block_info[0]++;

					if (plane == 0)
					{
						// register the first plane block only
						*bab = blk;					
						bab++;
					}
				} else {
					bdbm_error ("bdbm_abm_get_free_block_prepare failed");
					return 1;
				}
			}
		}
	}

	return 0;
}

bdbm_abm_block_t** __bdbm_page_ftl_create_active_blocks (
	bdbm_device_params_t* np,
	bdbm_abm_info_t* bai)
{
	uint64_t nr_punits;
	bdbm_abm_block_t** bab = NULL;

	nr_punits = np->nr_units_per_channel * np->nr_channels;

	/* create a set of active blocks */
	if ((bab = (bdbm_abm_block_t**)bdbm_zmalloc 
			(sizeof (bdbm_abm_block_t*) * nr_punits)) == NULL) {
		bdbm_error ("bdbm_zmalloc failed");
		goto fail;
	}

	/* get a set of free blocks for active blocks */
	if (__bdbm_page_ftl_get_active_blocks (np, bai, bab) != 0) {
		bdbm_error ("__bdbm_page_ftl_get_active_blocks failed");
		goto fail;
	}

	return bab;

fail:
	if (bab)
		bdbm_free (bab);
	return NULL;
}

void __bdbm_page_ftl_destroy_active_blocks (
	bdbm_abm_block_t** bab)
{
	if (bab == NULL)
		return;

	/* TODO: it might be required to save the status of active blocks 
	 * in order to support rebooting */
	bdbm_free (bab);
}

uint32_t bdbm_page_ftl_create (bdbm_drv_info_t* bdi)
{
	uint32_t i = 0, j = 0, k = 0;
	bdbm_page_ftl_private_t* p = NULL;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);

	/* create a private data structure */
	if ((p = (bdbm_page_ftl_private_t*)bdbm_zmalloc 
			(sizeof (bdbm_page_ftl_private_t))) == NULL) {
		bdbm_error ("bdbm_malloc failed");
		return 1;
	}
	p->curr_puid = 0;
	p->curr_page_ofs = 0;
	p->nr_punits = np->nr_units_per_channel * np->nr_channels;
	p->nr_punits_pages = p->nr_punits * np->nr_pages_per_block;
	bdbm_spin_lock_init (&p->ftl_lock);
	_ftl_page_ftl.ptr_private = (void*)p;

	/* create 'bdbm_abm_info' with pst */
	if ((p->bai = bdbm_abm_create (np, 1)) == NULL) {
		bdbm_error ("bdbm_abm_create failed");
		bdbm_page_ftl_destroy (bdi);
		return 1;
	}

	/* create a mapping table */
	if ((p->ptr_mapping_table = __bdbm_page_ftl_create_mapping_table (np)) == NULL) {
		bdbm_error ("__bdbm_page_ftl_create_mapping_table failed");
		bdbm_page_ftl_destroy (bdi);
		return 1;
	}

	if ((p->panMoveCount = (char*)(bdbm_zmalloc(np->nr_subpages_per_ssd))) != NULL)
	{
		int32_t idx;
		for (idx = 0; idx < np->nr_subpages_per_ssd; idx++)
		{
			p->panMoveCount[idx] = -1;
		}
	}
	else
	{
		bdbm_error ("alloc failed");
		bdbm_page_ftl_destroy (bdi);
		return 1;
	}
	

	/* allocate active blocks */
	if ((p->ac_bab = __bdbm_page_ftl_create_active_blocks (np, p->bai)) == NULL) {
		bdbm_error ("__bdbm_page_ftl_create_active_blocks failed");
		bdbm_page_ftl_destroy (bdi);
		return 1;
	}

	/* allocate gc stuff */
	if ((p->gc_src_bab = (bdbm_abm_block_t**)bdbm_zmalloc 
			(sizeof (bdbm_abm_block_t*) * p->nr_punits)) == NULL) {
		bdbm_error ("bdbm_zmalloc failed");
		bdbm_page_ftl_destroy (bdi);
		return 1;
	}

	for (i = 0; i < MAX_COPY_BACK; i++)
	{
		if ((p->gc_dst_bab[i] = (bdbm_abm_block_t**)bdbm_zmalloc 
				(sizeof (bdbm_abm_block_t*) * p->nr_punits)) == NULL) 
		{
			bdbm_error ("bdbm_zmalloc failed");
			bdbm_page_ftl_destroy (bdi);
			return 1;
		}

		if ((p->gc_dst_blk_offs[i] = (uint64_t*)bdbm_zmalloc
				(sizeof(uint64_t) * p->nr_punits)) == NULL)
		{
			bdbm_error ("bdbm_zmalloc failed");
			bdbm_page_ftl_destroy (bdi);		
		}

		p->block_info[i] = 0;
	}
	p->block_info[MAX_COPY_BACK] = p->nr_punits * np->nr_blocks_per_unit;

	// src block offset.
	for (i = 0; i < np->nr_planes; i++)
	{
		if ((p->gc_src_blk_offs[i] = (uint64_t*)bdbm_zmalloc
				(sizeof(uint64_t) * p->nr_punits)) == NULL)
		{
			bdbm_error ("bdbm_zmalloc failed");
			bdbm_page_ftl_destroy (bdi);		
		}
	}
	
//	for (i = 0; i < p->nr_punits; i++)
	for (i = 0; i < 64; i++)
	{
		for (k = 0; k < np->nr_planes; k++)
		{
			p->gc_src_blk_offs[k][i] = np->nr_pages_per_block;
		}
		
		for (j = 0; j < MAX_COPY_BACK; j++)
		{
			p->gc_dst_blk_offs[j][i] = np->nr_pages_per_block;	
		}

		p->host_write_count[i] = 0;
		p->total_write_count[i] = 0;
		p->invald_page_count[i] = 1; // prevent dividing by zero
	}
	
	if ((p->gc_hlm.llm_reqs = (bdbm_llm_req_t*)bdbm_zmalloc
			(sizeof (bdbm_llm_req_t) * p->nr_punits_pages)) == NULL) {
		bdbm_error ("bdbm_zmalloc failed");
		bdbm_page_ftl_destroy (bdi);
		return 1;
	}
	bdbm_sema_init (&p->gc_hlm.done);
	hlm_reqs_pool_allocate_llm_reqs (p->gc_hlm.llm_reqs, p->nr_punits_pages, RP_MEM_PHY);

	if ((p->gc_hlm_w.llm_reqs = (bdbm_llm_req_t*)bdbm_zmalloc
			(sizeof (bdbm_llm_req_t) * p->nr_punits_pages)) == NULL) {
		bdbm_error ("bdbm_zmalloc failed");
		bdbm_page_ftl_destroy (bdi);
		return 1;
	}
	bdbm_sema_init (&p->gc_hlm_w.done);
	hlm_reqs_pool_allocate_llm_reqs (p->gc_hlm_w.llm_reqs, p->nr_punits_pages, RP_MEM_PHY);


	p->src_valid= 0;
	p->src_valid_page_count = 0;
	p->src_unit_hand = 0;
	p->src_plane_hand = 0;

	p->partial_read_start = p->nr_punits * np->nr_planes;
	p->partial_read_end = p->nr_punits * 10;
	p->partial_head = p->partial_read_start;
	p->partial_tail = p->partial_read_start;
	p->required_subpage_count = 0;
	p->buffered_subpage_count = 0;

	p->erase_idx_start = p->partial_read_end;
	p->host_meta_idx_start = p->erase_idx_start + p->nr_punits;
	p->gc_meta_idx_start = p->host_meta_idx_start + p->nr_punits;
	p->meta_load_idx_start = p->gc_meta_idx_start  + p->nr_punits;

	p->cached_copyback_info = (uint8_t*)bdbm_zmalloc(np->nr_blocks_per_unit*np->nr_pages_per_block);
	bdbm_msg("addr : %llx, size: %lld", p->cached_copyback_info, np->nr_blocks_per_unit*np->nr_pages_per_block);

	p->dst_offset = 0;
	p->dst_index = 0;

	p->dma_write = 0;
	p->bypass_write = 0;
	
	p->external_die_difference_count = 0;
	p->external_partial_valid_count = 0;
	p->external_refresh_count = 0;	
	p->internal_copy_count = 0;
	
	p->gc_subpages_move_unit = p->nr_punits * np->nr_planes * np->nr_subpages_per_page;

	//p->src_unit_idx[0];
	p->gc_copy_count = 0;
	p->host_update_count = 0;

	p->nop_count = 0;
	p->gc_count = 0;
	p->utilization = 0;
	p->gc_mode = 0;

	p->alloc_idx = 0;

	p->token_mode = 0; // on / off
	p->token_count = 0; // valid only when token mode is on.
	p->generated_token = 0;
	
	for (i = 0; i < p->nr_punits; i++)
	{
		bdbm_llm_req_t* req;
		req = p->gc_hlm.llm_reqs + (p->host_meta_idx_start + i);
		hlm_reqs_pool_reset_fmain (&req->fmain, BDBM_MAX_PAGES);
		
		req = p->gc_hlm.llm_reqs + (p->gc_meta_idx_start + i);
		hlm_reqs_pool_reset_fmain (&req->fmain, BDBM_MAX_PAGES);
	}


#ifdef COPYBACK_QUOTA
	p->blk_quota_data = (quota_info*)bdbm_zmalloc(sizeof(quota_info)*np->nr_blocks_per_unit);

	p->distribution_info[0].threshold_copyback = 5;
	p->distribution_info[1].threshold_copyback = 4;
	p->distribution_info[2].threshold_copyback = 3;
	p->distribution_info[3].threshold_copyback = 2;
	p->distribution_info[4].threshold_copyback = 1;
	p->distribution_info[5].threshold_copyback = 0;

	p->new_quota = 0;
#if 1
	// case 0 - 0
	p->initial_quota = 12;
	p->distribution_info[0].proportion = 0;
	p->distribution_info[1].proportion = 99;
	p->distribution_info[2].proportion = 1;
	p->distribution_info[3].proportion = 0;
	p->distribution_info[4].proportion = 0;
	p->distribution_info[5].proportion = 0;

	p->quota_to_dstIdx[0] = 7;
	p->quota_to_dstIdx[1] = 7;
	p->quota_to_dstIdx[2] = 7;
	p->quota_to_dstIdx[3] = 6;
	p->quota_to_dstIdx[4] = 5;
	p->quota_to_dstIdx[5] = 4;	
	p->quota_to_dstIdx[6] = 3;		
	p->quota_to_dstIdx[7] = 3;		
	p->quota_to_dstIdx[8] = 2;		
	p->quota_to_dstIdx[9] = 1;				
	p->quota_to_dstIdx[10] = 1;				
	p->quota_to_dstIdx[11] = 1;	
	p->quota_to_dstIdx[12] = 0;

	p->dstIdx_to_quota[0] = 12;
	p->dstIdx_to_quota[1] = 9;
	p->dstIdx_to_quota[2] = 8;
	p->dstIdx_to_quota[3] = 6;
	p->dstIdx_to_quota[4] = 5;
	p->dstIdx_to_quota[5] = 4;
	p->dstIdx_to_quota[6] = 3;
	p->dstIdx_to_quota[7] = 0;

#elif 0 
	// case 1 - 1000
	p->initial_quota = 12;
	p->distribution_info[0].proportion = 0;
	p->distribution_info[1].proportion = 88;
	p->distribution_info[2].proportion = 11;
	p->distribution_info[3].proportion = 1;
	p->distribution_info[4].proportion = 0;
	p->distribution_info[5].proportion = 0;

	p->quota_to_dstIdx[0] = 7;
	p->quota_to_dstIdx[1] = 7;
	p->quota_to_dstIdx[2] = 7;
	p->quota_to_dstIdx[3] = 6;
	p->quota_to_dstIdx[4] = 5;
	p->quota_to_dstIdx[5] = 4;	
	p->quota_to_dstIdx[6] = 3;		
	p->quota_to_dstIdx[7] = 3;		
	p->quota_to_dstIdx[8] = 2;		
	p->quota_to_dstIdx[9] = 1;				
	p->quota_to_dstIdx[10] = 1;				
	p->quota_to_dstIdx[11] = 1;	
	p->quota_to_dstIdx[12] = 0;

	p->dstIdx_to_quota[0] = 12;
	p->dstIdx_to_quota[1] = 9;
	p->dstIdx_to_quota[2] = 8;
	p->dstIdx_to_quota[3] = 6;
	p->dstIdx_to_quota[4] = 5;
	p->dstIdx_to_quota[5] = 4;
	p->dstIdx_to_quota[6] = 3;
	p->dstIdx_to_quota[7] = 0;

#elif 0 
	// case 2 - 2000
	p->initial_quota = 12;
	p->distribution_info[0].proportion = 0;
	p->distribution_info[1].proportion = 30;
	p->distribution_info[2].proportion = 64;
	p->distribution_info[3].proportion = 6;
	p->distribution_info[4].proportion = 0;
	p->distribution_info[5].proportion = 0;

	p->quota_to_dstIdx[0] = 7;
	p->quota_to_dstIdx[1] = 7;
	p->quota_to_dstIdx[2] = 7;
	p->quota_to_dstIdx[3] = 6;
	p->quota_to_dstIdx[4] = 5;
	p->quota_to_dstIdx[5] = 4;	
	p->quota_to_dstIdx[6] = 3;		
	p->quota_to_dstIdx[7] = 3;		
	p->quota_to_dstIdx[8] = 2;		
	p->quota_to_dstIdx[9] = 1;				
	p->quota_to_dstIdx[10] = 1;				
	p->quota_to_dstIdx[11] = 1;	
	p->quota_to_dstIdx[12] = 0;

	p->dstIdx_to_quota[0] = 12;
	p->dstIdx_to_quota[1] = 9;
	p->dstIdx_to_quota[2] = 8;
	p->dstIdx_to_quota[3] = 6;
	p->dstIdx_to_quota[4] = 5;
	p->dstIdx_to_quota[5] = 4;
	p->dstIdx_to_quota[6] = 3;
	p->dstIdx_to_quota[7] = 0;

#elif 0
	// case 3 - 3000
	p->initial_quota = 6;
	p->distribution_info[0].proportion = 0;
	p->distribution_info[1].proportion = 0;
	p->distribution_info[2].proportion = 71;
	p->distribution_info[3].proportion = 26;
	p->distribution_info[4].proportion = 3;
	p->distribution_info[5].proportion = 0;

	p->quota_to_dstIdx[0] = 4;
	p->quota_to_dstIdx[1] = 4;
	p->quota_to_dstIdx[2] = 3;
	p->quota_to_dstIdx[3] = 2;
	p->quota_to_dstIdx[4] = 1;
	p->quota_to_dstIdx[5] = 1;	
	p->quota_to_dstIdx[6] = 0;		

	p->dstIdx_to_quota[0] = 6;
	p->dstIdx_to_quota[1] = 4;
	p->dstIdx_to_quota[2] = 3;
	p->dstIdx_to_quota[3] = 2;
	p->dstIdx_to_quota[4] = 0;
#elif 0	
	// case 4 - 4000
	p->initial_quota = 6;
	p->distribution_info[0].proportion = 0;
	p->distribution_info[1].proportion = 0;
	p->distribution_info[2].proportion = 15;
	p->distribution_info[3].proportion = 74;
	p->distribution_info[4].proportion = 11;
	p->distribution_info[5].proportion = 0;

	p->quota_to_dstIdx[0] = 4;
	p->quota_to_dstIdx[1] = 4;
	p->quota_to_dstIdx[2] = 3;
	p->quota_to_dstIdx[3] = 2;
	p->quota_to_dstIdx[4] = 1;
	p->quota_to_dstIdx[5] = 1;	
	p->quota_to_dstIdx[6] = 0;		

	p->dstIdx_to_quota[0] = 6;
	p->dstIdx_to_quota[1] = 4;
	p->dstIdx_to_quota[2] = 3;
	p->dstIdx_to_quota[3] = 2;
	p->dstIdx_to_quota[4] = 0;
#else	
	// case 5 - 5000
	p->initial_quota = 2;
	p->distribution_info[0].proportion = 0;
	p->distribution_info[1].proportion = 0;
	p->distribution_info[2].proportion = 0;
	p->distribution_info[3].proportion = 44;
	p->distribution_info[4].proportion = 53;
	p->distribution_info[5].proportion = 3;

	p->quota_to_dstIdx[0] = 2;
	p->quota_to_dstIdx[1] = 1;
	p->quota_to_dstIdx[2] = 0;

	p->dstIdx_to_quota[0] = 2;
	p->dstIdx_to_quota[1] = 1;
	p->dstIdx_to_quota[2] = 0;
#endif
	
	bdbm_msg(" initial quota : %lld",p->initial_quota);
	bdbm_msg(" %d, %d, %d, %d, %d, %d",		p->distribution_info[0].proportion,
		p->distribution_info[1].proportion,
		p->distribution_info[2].proportion,
		p->distribution_info[3].proportion,
		p->distribution_info[4].proportion,
		p->distribution_info[5].proportion);

	uint64_t idx;
	for (idx = 0; idx < np->nr_blocks_per_die; idx++)
	{
		uint32_t rand_value;
		uint32_t probability = 0;
		uint32_t array_idx;

		get_random_bytes(&rand_value, sizeof(uint32_t));
		rand_value %= 100;

		for (array_idx = 0; array_idx < 10; array_idx++)
		{
			probability += p->distribution_info[array_idx].proportion;

			if (rand_value < probability)
			{
				uint32_t threshold_copyback = p->distribution_info[array_idx].threshold_copyback;

#ifdef PER_BLOCK_QUOTA	
				p->blk_quota_data[idx].copyback_quota = p->initial_quota;
#endif				
				p->blk_quota_data[idx].threshold_copyback = threshold_copyback;

				if (threshold_copyback > 0)
				{
					p->blk_quota_data[idx].diminishing_quota = p->initial_quota / threshold_copyback;
				}
				else
				{
					p->blk_quota_data[idx].diminishing_quota = p->initial_quota + 1;
				}
				
				//bdbm_msg(" idx : %lld, c quota : %lld, d quota : %lld, threshold :%lld",
				//	idx, p->blk_quota_data[idx].copyback_quota,
				//	p->blk_quota_data[idx].diminishing_quota,
				//	p->blk_quota_data[idx].threshold_copyback);
					
				break;
			}
		}
	}

#endif


 
	return 0;
}

void bdbm_page_ftl_destroy (bdbm_drv_info_t* bdi)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;

	if (!p)
		return;

	uint32_t anHistogram[257] = {0,};
	uint32_t idx;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	for (idx = 0; idx < np->nr_subpages_per_ssd; idx++)
	{
		int32_t nMoveCount = p->panMoveCount[idx];
		if (nMoveCount >= 0)
		{
			anHistogram[nMoveCount]++;
		}
	}
	
	for (idx = 0; idx < 15; idx++)
	{
		bdbm_msg("move count : %lld, %lld", idx, anHistogram[idx]);
	}

	if (p->gc_hlm_w.llm_reqs) {
		hlm_reqs_pool_release_llm_reqs (p->gc_hlm_w.llm_reqs, p->nr_punits_pages, RP_MEM_PHY);
		bdbm_sema_free (&p->gc_hlm_w.done);
		bdbm_free (p->gc_hlm_w.llm_reqs);
	}
	if (p->gc_hlm.llm_reqs) {
		hlm_reqs_pool_release_llm_reqs (p->gc_hlm.llm_reqs, p->nr_punits_pages, RP_MEM_PHY);
		bdbm_sema_free (&p->gc_hlm.done);
		bdbm_free (p->gc_hlm.llm_reqs);
	}
	if (p->gc_src_bab)
		bdbm_free (p->gc_src_bab);
	if (p->ac_bab)
		__bdbm_page_ftl_destroy_active_blocks (p->ac_bab);
	if (p->ptr_mapping_table)
		__bdbm_page_ftl_destroy_mapping_table (p->ptr_mapping_table);
	if (p->panMoveCount)
		bdbm_free (p->panMoveCount);
	if (p->bai)
		bdbm_abm_destroy (p->bai);
	bdbm_free (p);
}

void bdbm_page_ftl_print_llm(bdbm_llm_req_t* llm_reqs, uint64_t size)
{
	uint64_t index;
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;

	for (index = 0; index < size; index++)
	{
		if (llm_reqs[index].req_type != 0)
		{
			bdbm_msg("index: %lld, reqType:%x, ch:%lld, way:%lld, GC %lld", index, llm_reqs[index].req_type, llm_reqs[index].phyaddr.channel_no, llm_reqs[index].phyaddr.way_no, p->gc_count);
		}
	}
}

void bdbm_page_ftl_print_blocks(bdbm_drv_info_t* bdi)
{
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	uint64_t ch, way, unit;

	bdbm_msg("Allocated Blocks");
	for (ch = 0; ch < np->nr_channels;	ch++)
	{
		for (way = 0; way < np->nr_units_per_channel; way++)
		{	
			unit = ch * np->nr_units_per_channel + way;

			if ((p->ac_bab[unit] != NULL) && 
				(p->gc_src_bab[unit]!= NULL) && 
				(p->gc_dst_bab[0][unit]!= NULL))
			{

			bdbm_msg("ch:%lld, way:%lld, %lld,%lld,%lld,%lld",ch, way,
				(p->ac_bab[unit])->block_no,
				(p->gc_src_bab[unit])->block_no,
				(p->gc_dst_bab[0][unit])->block_no, unit);
				//(p->gc_dst_bab[1][ch*np->nr_units_per_channel + way])->block_no,
				//(p->gc_dst_bab[2][ch*np->nr_units_per_channel + way])->block_no);
			}
		}
	}
}
void bdbm_page_ftl_print_freeblocks(bdbm_drv_info_t* bdi)
{
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;

	uint64_t ch;

	bdbm_msg("FreeBlock");
	for (ch = 0; ch < np->nr_channels;	ch++)
	{				
		bdbm_msg("%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld",
				p->bai->anr_free_blks[ch][0], 
				p->bai->anr_free_blks[ch][1], 
				p->bai->anr_free_blks[ch][2], 
				p->bai->anr_free_blks[ch][3], 
				p->bai->anr_free_blks[ch][4], 
				p->bai->anr_free_blks[ch][5], 
				p->bai->anr_free_blks[ch][6], 
				p->bai->anr_free_blks[ch][7]);
	}
}


void bdbm_page_ftl_print_totalEC(bdbm_drv_info_t* bdi)
{
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;

	uint64_t ch, way, blk;
	uint64_t blk_idx;
	bdbm_abm_block_t* pblk = NULL;
	uint64_t erase_count[64];
	uint64_t unit;

	for (ch = 0; ch < np->nr_channels;	ch++)
	{	
		for (way = 0; way < np->nr_units_per_channel/np->nr_groups_per_die; way++)
		{
			uint64_t nTotalEC = 0;
			unit =  (ch * np->nr_units_per_channel + way);
			
			for (blk = 0; blk < np->nr_blocks_per_die; blk++)
			{
				blk_idx = ch* np->nr_blocks_per_channel + way * np->nr_blocks_per_die + blk;

				pblk = p->bai->blocks + blk_idx;
				nTotalEC += pblk->erase_count;
			}

			erase_count[unit] = nTotalEC;
		}
	}		

	bdbm_msg("EC, %lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld, %lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld, %lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld, %lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,  %lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld, %lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld, %lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld, %lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld",
		erase_count[0],erase_count[1],erase_count[2],erase_count[3],erase_count[4],erase_count[5],erase_count[6],erase_count[7],
		erase_count[8],erase_count[9],erase_count[10],erase_count[11],erase_count[12],erase_count[13],erase_count[14],erase_count[15],
		erase_count[16],erase_count[17],erase_count[18],erase_count[19],erase_count[20],erase_count[21],erase_count[22],erase_count[23],
		erase_count[24],erase_count[25],erase_count[26],erase_count[27],erase_count[28],erase_count[29],erase_count[30],erase_count[31],
		erase_count[32],erase_count[33],erase_count[34],erase_count[35],erase_count[36],erase_count[37],erase_count[38],erase_count[39],
		erase_count[40],erase_count[41],erase_count[42],erase_count[43],erase_count[44],erase_count[45],erase_count[46],erase_count[47],
		erase_count[48],erase_count[49],erase_count[50],erase_count[51],erase_count[52],erase_count[53],erase_count[54],erase_count[55],
		erase_count[56],erase_count[57],erase_count[58],erase_count[59],erase_count[60],erase_count[61],erase_count[62],erase_count[63]);
}

void bdbm_page_ftl_print_hostWrite(void)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;

	if (p->nr_punits == 64)
	{
		bdbm_msg("hostW, %lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld, %lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,  %lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld, %lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,   %lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld, %lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,  %lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld, %lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld", 
			p->host_write_count[0],p->host_write_count[1],p->host_write_count[2],p->host_write_count[3],p->host_write_count[4],p->host_write_count[5],p->host_write_count[6],p->host_write_count[7], 
			p->host_write_count[8],p->host_write_count[9],p->host_write_count[10],p->host_write_count[11],p->host_write_count[12],p->host_write_count[13],p->host_write_count[14],p->host_write_count[15], 
			p->host_write_count[16],p->host_write_count[17],p->host_write_count[18],p->host_write_count[19],p->host_write_count[20],p->host_write_count[21],p->host_write_count[22],p->host_write_count[23], 
			p->host_write_count[24],p->host_write_count[25],p->host_write_count[26],p->host_write_count[27],p->host_write_count[28],p->host_write_count[29],p->host_write_count[30],p->host_write_count[31], 
			p->host_write_count[32],p->host_write_count[33],p->host_write_count[34],p->host_write_count[35],p->host_write_count[36],p->host_write_count[37],p->host_write_count[38],p->host_write_count[39], 
			p->host_write_count[40],p->host_write_count[41],p->host_write_count[42],p->host_write_count[43],p->host_write_count[44],p->host_write_count[45],p->host_write_count[46],p->host_write_count[47], 
			p->host_write_count[48],p->host_write_count[49],p->host_write_count[50],p->host_write_count[51],p->host_write_count[52],p->host_write_count[53],p->host_write_count[54],p->host_write_count[55], 
			p->host_write_count[56],p->host_write_count[57],p->host_write_count[58],p->host_write_count[59],p->host_write_count[60],p->host_write_count[61],p->host_write_count[62],p->host_write_count[63]);
	}
	else if (p->nr_punits == 32)
	{
		bdbm_msg("hostW, %lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld, %lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,  %lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld, %lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld",  
			p->host_write_count[0],p->host_write_count[1],p->host_write_count[2],p->host_write_count[3],p->host_write_count[4],p->host_write_count[5],p->host_write_count[6],p->host_write_count[7], 
			p->host_write_count[8],p->host_write_count[9],p->host_write_count[10],p->host_write_count[11],p->host_write_count[12],p->host_write_count[13],p->host_write_count[14],p->host_write_count[15], 
			p->host_write_count[16],p->host_write_count[17],p->host_write_count[18],p->host_write_count[19],p->host_write_count[20],p->host_write_count[21],p->host_write_count[22],p->host_write_count[23], 
			p->host_write_count[24],p->host_write_count[25],p->host_write_count[26],p->host_write_count[27],p->host_write_count[28],p->host_write_count[29],p->host_write_count[30],p->host_write_count[31]);
	}						
	else
	{
		bdbm_msg("hostW, %lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld, %lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld",
			p->host_write_count[0],p->host_write_count[1],p->host_write_count[2],p->host_write_count[3],p->host_write_count[4],p->host_write_count[5],p->host_write_count[6],p->host_write_count[7], 
			p->host_write_count[8],p->host_write_count[9],p->host_write_count[10],p->host_write_count[11],p->host_write_count[12],p->host_write_count[13],p->host_write_count[14],p->host_write_count[15]); 
	}
}

void bdbm_page_ftl_print_WAF(void)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;

	bdbm_msg("WAF %lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld, %lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld, %lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld, %lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld, %lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld, %lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld, %lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld, %lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld", 
	(p->total_write_count[0])*100/p->invald_page_count[0], (p->total_write_count[1])*100/p->invald_page_count[1],(p->total_write_count[2])*100/p->invald_page_count[2],(p->total_write_count[3])*100/p->invald_page_count[3],p->total_write_count[4]*100/p->invald_page_count[4],p->total_write_count[5]*100/p->invald_page_count[5],p->total_write_count[6]*100/p->invald_page_count[6],
		p->total_write_count[7]*100/p->invald_page_count[7], p->total_write_count[8]*100/p->invald_page_count[8],p->total_write_count[9]*100/p->invald_page_count[9],p->total_write_count[10]*100/p->invald_page_count[10],p->total_write_count[11]*100/p->invald_page_count[11],p->total_write_count[12]*100/p->invald_page_count[12],p->total_write_count[13]*100/p->invald_page_count[13],p->total_write_count[14]*100/p->invald_page_count[14],p->total_write_count[15]*100/p->invald_page_count[15], p->total_write_count[16]*100/p->invald_page_count[16],p->total_write_count[17]*100/p->invald_page_count[17],p->total_write_count[18]*100/p->invald_page_count[18],p->total_write_count[19]*100/p->invald_page_count[19],p->total_write_count[20]*100/p->invald_page_count[20],p->total_write_count[21]*100/p->invald_page_count[21],p->total_write_count[22]*100/p->invald_page_count[22],p->total_write_count[23]*100/p->invald_page_count[23],p->total_write_count[24]*100/p->invald_page_count[24],p->total_write_count[25]*100/p->invald_page_count[25],p->total_write_count[26]*100/p->invald_page_count[26], p->total_write_count[27]*100/p->invald_page_count[27],		p->total_write_count[28]*100/p->invald_page_count[28],		p->total_write_count[29]*100/p->invald_page_count[29],		p->total_write_count[30]*100/p->invald_page_count[30],		p->total_write_count[31]*100/p->invald_page_count[31],		p->total_write_count[32]*100/p->invald_page_count[32],		p->total_write_count[33]*100/p->invald_page_count[33],		p->total_write_count[34]*100/p->invald_page_count[34],		p->total_write_count[35]*100/p->invald_page_count[35],		p->total_write_count[36]*100/p->invald_page_count[36],		p->total_write_count[37]*100/p->invald_page_count[37],		p->total_write_count[38]*100/p->invald_page_count[38],		p->total_write_count[39]*100/p->invald_page_count[39],		p->total_write_count[40]*100/p->invald_page_count[40],		p->total_write_count[41]*100/p->invald_page_count[41],		p->total_write_count[42]*100/p->invald_page_count[42],		p->total_write_count[43]*100/p->invald_page_count[43],		p->total_write_count[44]*100/p->invald_page_count[44],		p->total_write_count[45]*100/p->invald_page_count[45],		p->total_write_count[46]*100/p->invald_page_count[46],		p->total_write_count[47]*100/p->invald_page_count[47],		p->total_write_count[48]*100/p->invald_page_count[48],		p->total_write_count[49]*100/p->invald_page_count[49],		p->total_write_count[50]*100/p->invald_page_count[50],		p->total_write_count[51]*100/p->invald_page_count[51],		p->total_write_count[52]*100/p->invald_page_count[52],		p->total_write_count[53]*100/p->invald_page_count[53],		p->total_write_count[54]*100/p->invald_page_count[54],		p->total_write_count[55]*100/p->invald_page_count[55],		p->total_write_count[56]*100/p->invald_page_count[56],		p->total_write_count[57]*100/p->invald_page_count[57],		p->total_write_count[58]*100/p->invald_page_count[58],p->total_write_count[59]*100/p->invald_page_count[59],		p->total_write_count[60]*100/p->invald_page_count[60],p->total_write_count[61]*100/p->invald_page_count[61],p->total_write_count[62]*100/p->invald_page_count[62],p->total_write_count[63]*100/p->invald_page_count[63]);

}

void bdbm_page_ftl_print_copyback_info(bdbm_drv_info_t* bdi)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
#if 0
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	bdbm_abm_block_t* cur_blk = NULL;
	struct list_head* pos = NULL;

	uint64_t ch, way;
	uint64_t blk_info[6] = {0, };

	if (p->bai->nr_dirty_blks != 0)
	{	
		for (ch = 0; ch < np->nr_channels; ch++)
		{
			for (way = 0; way < np->nr_units_per_channel; way++)
			{
				bdbm_abm_list_for_each_dirty_block (pos, p->bai, ch, way) 
				{
					cur_blk = bdbm_abm_fetch_dirty_block (pos);

					blk_info[cur_blk->copy_count]++;
				}
			}
		}

		bdbm_msg(" Blk_distribution:%lld,%lld,%lld,%lld,%lld,%lld", blk_info[0],blk_info[1],blk_info[2],blk_info[3],blk_info[4],blk_info[5]);
	}
#else

	
	bdbm_msg("S:%lld, %lld,%lld,%lld,%lld,%lld,%lld, gc %lld",p->alloc_idx, p->block_info[0],p->block_info[1],p->block_info[2],p->block_info[3],p->block_info[4],p->block_info[5], p->gc_count );

#endif


}


uint32_t bdbm_page_ftl_get_free_ppa (
	bdbm_drv_info_t* bdi, 
	int64_t lpa,
	bdbm_phyaddr_t* ppa)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	bdbm_abm_block_t* b = NULL;
	uint64_t curr_channel;
	uint64_t curr_way;
	uint64_t curr_unit;
	
	/* get the channel & unit numbers */
	curr_channel = p->curr_puid % np->nr_channels;
	curr_way = (p->curr_puid / np->nr_channels) % np->nr_ways ;
	curr_unit = curr_way * np->nr_groups_per_die + (p->curr_puid/np->nr_channels)/np->nr_ways ;


	/* get the physical offset of the active blocks */
	b = p->ac_bab[curr_channel * np->nr_units_per_channel + curr_unit];
	ppa->channel_no =  b->channel_no;
	ppa->way_no = b->way_no;
	ppa->block_no = b->block_no;
	ppa->page_no = p->curr_page_ofs;
	ppa->punit_id = BDBM_GET_PUNIT_ID (bdi, ppa);

	bdbm_msg("ch %d, way %d, unit %d, blk %d, page %d", ppa->channel_no, ppa->way_no, ppa->punit_id, ppa->block_no, ppa->page_no);

	/* check some error cases before returning the physical address */
	bdbm_bug_on (ppa->channel_no != curr_channel);
	bdbm_bug_on (ppa->way_no != curr_way);
	bdbm_bug_on (ppa->unit_no != curr_unit);
	bdbm_bug_on (ppa->page_no >= np->nr_pages_per_block);

	/* go to the next parallel unit */
	if ((p->curr_puid + 1) == p->nr_punits) {
		p->curr_puid = 0;
		p->curr_page_ofs++;	/* go to the next page */

		/* see if there are sufficient free pages or not */
		if (p->curr_page_ofs == np->nr_pages_per_block) {
			/* get active blocks */
			if (__bdbm_page_ftl_get_active_blocks (np, p->bai, p->ac_bab) != 0) {
				//bdbm_error ("__bdbm_page_ftl_get_active_blocks failed");

				p->curr_page_ofs--;	/* adjust to previous location */
				p->curr_puid = p->nr_punits - 1;
				return 1;
			}
			/* ok; go ahead with 0 offset */
			/*bdbm_msg ("curr_puid = %llu", p->curr_puid);*/
			p->curr_page_ofs = 0;
			
			//bdbm_page_ftl_print_hostWrite();
			//bdbm_page_ftl_print_WAF();
			//bdbm_page_ftl_print_totalEC(bdi);
			//bdbm_msg("Alloc_HostFreeBlk");
			bdbm_page_ftl_print_copyback_info(bdi);

			p->alloc_idx++;


			if ( p->gc_copy_count != 0)
			{
				p->host_update_count += (np->nr_subpages_per_block * p->nr_punits * np->nr_planes);
				bdbm_msg("T %lld  I %lld D %lld P %lld R %lld  %lld : %lld  %4lld", p->gc_copy_count, p->internal_copy_count, p->external_die_difference_count, p->external_partial_valid_count, p->external_refresh_count, p->internal_copy_count *10000/p->gc_copy_count, p->host_update_count, (p->host_update_count+p->gc_copy_count)*1000/p->host_update_count);	
			}
#ifdef PER_BLOCK_QUOTA			
			p->blk_quota_data[p->ac_bab[0]->block_no].copyback_quota = p->initial_quota;
#endif
		}
	} else {
		/*bdbm_msg ("curr_puid = %llu", p->curr_puid);*/
		p->curr_puid++;
	}

	p->host_write_count[lpa % p->nr_punits]++;

	return 0;
}

uint32_t bdbm_page_ftl_get_free_ppa_gc (
	bdbm_drv_info_t* bdi, 
	uint64_t unit,
	uint64_t copy_count,
	bdbm_phyaddr_t* ppa)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	bdbm_abm_block_t* b = NULL;
	uint64_t plane;
				
	if (p->gc_dst_blk_offs[copy_count][unit] == np->nr_pages_per_block)
	{
		uint64_t ch = unit/np->nr_units_per_channel;
		uint64_t way = unit%np->nr_units_per_channel;
		
		/* restore previous active block */
		if (p->gc_dst_bab[copy_count][unit] != NULL)
		{
			for (plane = 0; plane < np->nr_planes; plane++)
			{
				b = p->gc_dst_bab[copy_count][unit];
//				bdbm_msg("restore dest : %lld, invalid : %lld", b[plane].block_no, b[plane].nr_invalid_subpages);

				bdbm_abm_make_dirty_blk(p->bai, ch, way, b[plane].block_no);
			}
		}
	
		if (unit == 0)
		{
//			bdbm_msg("Dest blk : cpcnt:%lld, free:%lld, plane0:%lld, page1:%lld", copy_count, p->bai->nr_free_blks, p->bai->anr_free_blks[0][0], p->bai->anr_free_blks[1][0]);		
		}

		for (plane = 0; plane < np->nr_planes; plane++)
		{
			b = bdbm_abm_get_free_block_prepare (p->bai, ch, way);
			if (b == NULL)
			{
				bdbm_error ("bdbm_abm_get_free_block_prepare failed");
				return 1; 
			}

			if (plane == 0)
			{
				p->gc_dst_bab[copy_count][unit] = b;
			}

			bdbm_abm_get_free_block_commit (p->bai, b);
			b->info = 11;//dest
			b->copy_count = copy_count;
			p->block_info[copy_count]++;

#ifdef PER_BLOCK_QUOTA
			if (unit == 0)
			{
				p->blk_quota_data[b->block_no + plane].copyback_quota = p->dstIdx_to_quota[copy_count];

				//bdbm_msg( "new dst - idx : %lld, quota : %lld", copy_count, p->dstIdx_to_quota[copy_count]);
			}
#endif			
		}
				
		/* ok; go ahead with 0 offset */ 
		p->gc_dst_blk_offs[copy_count][unit] = 0;

		if (unit == 0)
		{
//			bdbm_msg("Dest blk : %llu", b->block_no);			
		}
	}

	/* get the physical offset of the active blocks */
	b = p->gc_dst_bab[copy_count][unit];

	ppa->channel_no =  b->channel_no;
	ppa->way_no = b->way_no;
	ppa->block_no = b->block_no;
	ppa->page_no = p->gc_dst_blk_offs[copy_count][unit];
	ppa->punit_id = BDBM_GET_PUNIT_ID (bdi, ppa); // unit

	p->gc_dst_blk_offs[copy_count][unit]++;

	return 0;
}

uint32_t bdbm_page_ftl_map_lpa_to_ppa (
	bdbm_drv_info_t* bdi, 
	bdbm_logaddr_t* logaddr,
	bdbm_phyaddr_t* phyaddr,
	uint32_t info)
{
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_page_mapping_entry_t* me = NULL;
	uint64_t subpage;
	uint64_t plane;

	for (plane = 0; plane < np->nr_planes; plane++)
	{
		/* is it a valid logical address */
		for (subpage = 0; subpage < np->nr_subpages_per_page; subpage++) 
		{
			uint64_t index = plane*np->nr_subpages_per_page + subpage;
			
			if (logaddr->lpa[index] == -1) 
			{
				/* the correpsonding subpage must be set to invalid for gc */
				bdbm_abm_invalidate_page (
					p->bai, 
					phyaddr->channel_no, 
					phyaddr->way_no,
					phyaddr->block_no + plane,
					phyaddr->page_no,
					subpage
				);

				continue;
			}

			if (logaddr->lpa[index] >= np->nr_subpages_per_ssd) 
			{
				bdbm_error ("LPA is beyond logical space (%llX)", logaddr->lpa[index]);
				return 1;
			}

			if (info == 0)
			{	// host write
				p->panMoveCount[logaddr->lpa[index]] = 0;
			}
			else
			{
				// GC write
				p->panMoveCount[logaddr->lpa[index]]++;
			}

			/* get the mapping entry for lpa */
			me = &p->ptr_mapping_table[logaddr->lpa[index]];
			bdbm_bug_on (me == NULL);

			/* update the mapping table */
			if (me->status == PFTL_PAGE_VALID) {
				bdbm_abm_invalidate_page (
					p->bai, 
					me->phyaddr.channel_no, 
					me->phyaddr.way_no,
					me->phyaddr.block_no,
					me->phyaddr.page_no,
					me->sp_off
				);
			}
			me->status = PFTL_PAGE_VALID;
			me->phyaddr.channel_no = phyaddr->channel_no;
			me->phyaddr.way_no = phyaddr->way_no;
			me->phyaddr.block_no = phyaddr->block_no + plane;
			me->phyaddr.page_no = phyaddr->page_no;
			me->sp_off = subpage;
		}
	}
	
	return 0;
}

	uint32_t bdbm_page_ftl_get_ppa (
		bdbm_drv_info_t* bdi, 
		int64_t lpa,
		bdbm_phyaddr_t* phyaddr,
		uint64_t* sp_off)
	{
		bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
		bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
		bdbm_page_mapping_entry_t* me = NULL;
		uint32_t ret;

		/* is it a valid logical address */
		if (lpa >= np->nr_subpages_per_ssd) {
			bdbm_error ("A given lpa is beyond logical space (%llu)", lpa);
			return 1;
		}

		/* get the mapping entry for lpa */
		me = &p->ptr_mapping_table[lpa];

		/* NOTE: sometimes a file system attempts to read 
		 * a logical address that was not written before.
		 * in that case, we return 'address 0' */
		if (me->status != PFTL_PAGE_VALID) {
			phyaddr->channel_no = 0;
			phyaddr->way_no = 0;
			phyaddr->block_no = 0;
			phyaddr->page_no = 0;
			phyaddr->punit_id = 0;
			*sp_off = 0;
			ret = 1;
		} else {
			phyaddr->channel_no = me->phyaddr.channel_no;
			phyaddr->way_no = me->phyaddr.way_no;
			phyaddr->block_no = me->phyaddr.block_no;
			phyaddr->page_no = me->phyaddr.page_no;
			phyaddr->punit_id = BDBM_GET_PUNIT_ID (bdi, phyaddr);
			*sp_off = me->sp_off;
			ret = 0;
		}

		return ret;
	}

	uint32_t bdbm_page_ftl_invalidate_lpa (
		bdbm_drv_info_t* bdi, 
		int64_t lpa, 
		uint64_t len)
	{	
		bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
		bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
		bdbm_page_mapping_entry_t* me = NULL;
		uint64_t loop;

		/* check the range of input addresses */
		if ((lpa + len) > np->nr_subpages_per_ssd) {
			bdbm_warning ("LPA is beyond logical space (%llu = %llu+%llu) %llu", 
				lpa+len, lpa, len, np->nr_subpages_per_ssd);
			return 1;
		}

		/* make them invalid */
		for (loop = lpa; loop < (lpa + len); loop++) {
			me = &p->ptr_mapping_table[loop];
			if (me->status == PFTL_PAGE_VALID) {
				bdbm_abm_invalidate_page (
					p->bai, 
					me->phyaddr.channel_no, 
					me->phyaddr.way_no,
					me->phyaddr.block_no,
					me->phyaddr.page_no,
					me->sp_off
				);
				me->status = PFTL_PAGE_INVALID;
			}
		}

		return 0;
	}

	uint8_t bdbm_page_ftl_is_gc_needed (bdbm_drv_info_t* bdi, int64_t lpa)
	{
		bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
		uint64_t nr_free_blks = bdbm_abm_get_nr_free_blocks (p->bai);

		/* invoke gc when remaining free blocks are less than 1% of total blocks */
		if (nr_free_blks <= p->bai->nr_gc_ondemand_threshold)
		{
			if (p->token_mode == 0)
			{
				p->token_mode = 1;
				p->token_count = 0;
			}
			// on demand GC.
			return ON_DEMAND_GC;
		}
		else if (nr_free_blks < p->bai->nr_gc_background_threshold)
		{
			p->token_mode = 0;
			
			// back ground GC.
			return BACKGROUND_GC;
		}

		return 0;
	}

	/* VICTIM SELECTION - First Selection:
	 * select the first dirty block in a list */
	bdbm_abm_block_t* __bdbm_page_ftl_victim_selection (
		bdbm_drv_info_t* bdi,
		uint64_t channel_no,
		uint64_t unit_no)
	{
		bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
		bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
		bdbm_abm_block_t* a = NULL;
		bdbm_abm_block_t* b = NULL;
		struct list_head* pos = NULL;

		a = p->ac_bab[channel_no*np->nr_units_per_channel + unit_no];
		bdbm_abm_list_for_each_dirty_block (pos, p->bai, channel_no, unit_no) {
			b = bdbm_abm_fetch_dirty_block (pos);
			if (a != b)
				break;
			b = NULL;
		}

		return b;
	}


/* VICTIM SELECTION - Greedy:
 * select a dirty block with a small number of valid pages */
bdbm_abm_block_t* __bdbm_page_ftl_victim_selection_greedy (
	bdbm_drv_info_t* bdi,
	uint64_t channel_no,
	uint64_t unit_no)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);

	bdbm_abm_block_t* victim = NULL;
	struct list_head* pos;

	uint64_t unit = channel_no * np->nr_units_per_channel + unit_no;
	uint64_t plane;
	uint64_t full_invalid_pages = np->nr_subpages_per_block * np->nr_planes * p->nr_punits; 

	static uint64_t valid_victim = 0;
	static uint64_t victim_blk_no = 0;

	uint64_t index;
	uint64_t anMax_invalid_pages[MAX_COPY_BACK];
	bdbm_abm_block_t* apVictim[MAX_COPY_BACK]; 
	uint64_t max_generation_factor = 0;

	for (index = 0; index < MAX_COPY_BACK; index++)
	{
		anMax_invalid_pages[index] = 0;
		apVictim[index] = NULL;
	}

	if (valid_victim != 0)
	{	
		victim = bdbm_abm_get_block(p->bai, channel_no, unit_no/np->nr_groups_per_die, victim_blk_no);

		if ((unit + 1) == p->nr_punits)
		{
			valid_victim = 0; // this victim information is not valid anymore. 
		}
	
		return victim;
	}

	bdbm_abm_list_for_each_dirty_block (pos, p->bai, 0, 0) 
	{
		uint64_t invalid_pages = 0;
		uint64_t blk_info;
		bdbm_abm_block_t* block = bdbm_abm_fetch_dirty_block (pos);

		invalid_pages = p->bai->pnr_blk_invalid[block->block_no/PLANE_NUMBER]; 	
		for (plane = 1; plane < np->nr_planes; plane++)
		{
			pos = pos->next; // next plane;
		}		
	
		blk_info = block->copy_count;
		if (apVictim[blk_info] == NULL)
		{
			apVictim[blk_info] = block;
			anMax_invalid_pages[blk_info] = invalid_pages;
		}
		else if (invalid_pages > anMax_invalid_pages[blk_info])
		{
			apVictim[blk_info] = block;
			anMax_invalid_pages[blk_info] = invalid_pages;
		}

		if (invalid_pages == full_invalid_pages) 
		{
			break;
		}
	}

	for (index = 0; index < MAX_COPY_BACK; index++)
	{	
		uint64_t dst_idx;
		uint64_t generation_factor;

		if (apVictim[index] == NULL)
		{
			continue;
		}

		dst_idx = index + 1;
		if (dst_idx == MAX_COPY_BACK)
		{
			dst_idx = 0;
		}

		
		generation_factor = anMax_invalid_pages[index];

#if	(LAZY_CORRECTION_MODE != 0)
		if ( (p->utilization >= UTILIZATION_LAZY_MODE) &&
			 (p->block_info[MAX_COPY_BACK-1] < (p->block_info[MAX_COPY_BACK]/MAX_COPY_BACK)* LAZY_MODE_THRESHOLD) )
		{
			// lazy mode condition
			if (dst_idx == 0)
			{
				generation_factor = (generation_factor * GENERATION_FACTOR_WEIGHT);
				p->gc_mode = 1;	// lazy correction mode
			}
		}
#endif

//		cost = anMax_invalid_pages[index] + (np->nr_pages_per_block - p->gc_dst_blk_offs[dst_idx][0])*GC_FACTOR / np->nr_pages_per_block;
	
		if (max_generation_factor < generation_factor)
		{
			max_generation_factor = generation_factor;
			victim = apVictim[index];
		}
	}

	valid_victim = 1;
	victim_blk_no = victim->block_no;

	return victim;
}


void bdbm_page_ftl_restore_srcblk(bdbm_drv_info_t* bdi)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	bdbm_hlm_req_gc_t* hlm_gc = &p->gc_hlm; // used for readi
	uint64_t unit;
	uint64_t plane;		
	bdbm_llm_req_t* req;
	bdbm_abm_block_t* src_blk;

	for (unit = 0; unit < p->nr_punits; unit++)
	{
		src_blk = p->gc_src_bab[unit]; 

		// 1. Erase and Restore
		if (src_blk != NULL)
		{
			uint64_t erase_req_idx = p->erase_idx_start + unit;

			if (src_blk->nr_invalid_subpages != np->nr_subpages_per_block)
			{
				bdbm_msg("%lld, %d",unit,src_blk->nr_invalid_subpages);  
				bdbm_bug_on(1);
			}
				
			// src is already invalided.
			req = &hlm_gc->llm_reqs[erase_req_idx];
			req->req_type = REQTYPE_GC_ERASE;
			req->logaddr.lpa[0] = 0x7FFFFFFE - unit; /* lpa is not available now */
			req->phyaddr.channel_no = src_blk->channel_no;
			req->phyaddr.way_no = src_blk->way_no;
			req->phyaddr.block_no = src_blk->block_no;
			req->phyaddr.page_no = 0;
			req->phyaddr.punit_id = BDBM_GET_PUNIT_ID (bdi, (&req->phyaddr));
			req->ptr_hlm_req = (void*)hlm_gc;
			req->ret = 0;
		
			/* send erase reqs to llm */
			hlm_gc->req_type = REQTYPE_GC_ERASE;
			hlm_gc->nr_llm_reqs++; // Need to care Erase case ?
			
			if ((bdi->ptr_llm_inf->make_req (bdi, req)) != 0) {
				bdbm_error ("llm_make_req failed");
				bdbm_bug_on (1);
			}

			for (plane = 0; plane < np->nr_planes; plane++)
			{
				bdbm_abm_erase_block (p->bai, src_blk->channel_no, src_blk->way_no, src_blk->block_no + plane, /*is bad*/ 0);
				p->block_info[src_blk->copy_count]--;
				
				// adjust block page offset.
				p->gc_src_blk_offs[plane][unit] = np->nr_pages_per_block;
			}
			
			p->gc_src_bab[unit] = NULL;
			src_blk = NULL;
		}
	}
}

void bdbm_page_ftl_alloc_srcblk(bdbm_drv_info_t* bdi)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	uint64_t ch, way, unit;
	bdbm_abm_block_t* src_blk = NULL;

	p->gc_mode = 0;	// default correction mode

	p->generated_token = 0;
	for (unit = 0; unit < p->nr_punits; unit++)
	{
		src_blk = p->gc_src_bab[unit]; 

		// 2. Alloc New Src
		bdbm_bug_on(p->gc_src_blk_offs[0][unit] != np->nr_pages_per_block);
		
		ch = unit / np->nr_units_per_channel;
		way = unit % np->nr_units_per_channel; 
		
		// choose victim blocks - new src block
		if ((src_blk = __bdbm_page_ftl_victim_selection_greedy (bdi, ch, way))) 
		{
			uint64_t plane;
			
			p->gc_src_bab[unit] = src_blk;

			for (plane = 0; plane < np->nr_planes; plane++)
			{
				p->gc_src_blk_offs[plane][unit] = 0;

				if (src_blk[plane].nr_invalid_subpages == np->nr_subpages_per_block)
				{
					p->gc_src_blk_offs[plane][unit] = np->nr_pages_per_block;
				}

				p->total_write_count[unit] += np->nr_subpages_per_block;
				p->invald_page_count[unit] += src_blk[plane].nr_invalid_subpages;

				p->src_valid_page_count += (np->nr_subpages_per_block - src_blk[plane].nr_invalid_subpages);

//					bdbm_msg(  "unit:%lld, plane:%lld, valid:%lld, blk: %lld, info:%lld", unit, plane, (np->nr_subpages_per_block - src_blk[plane].nr_invalid_subpages), src_blk[plane].block_no, src_blk[plane].info);
#ifdef PER_PAGE_COPYBACK_MANAGEMENT
				p->generated_token += (src_blk[plane].nr_invalid_subpages - np->nr_subpages_per_page);
#else
				p->generated_token += src_blk[plane].nr_invalid_subpages;
#endif
			}
		}
	}
	
	p->generated_token /= np->nr_pages_per_block;
	p->generated_token--;

#ifndef COPYBACK_QUOTA	
	p->dst_index = src_blk[0].copy_count + 1;
	if (p->dst_index == MAX_COPY_BACK)
	{
		p->dst_index = 0;
	}
#else

#ifdef	PER_BLOCK_QUOTA
	int32_t newQuota = p->blk_quota_data[src_blk[0].block_no].copyback_quota - p->blk_quota_data[src_blk[0].block_no].diminishing_quota;

	if (newQuota >= 0)
	{
		p->dst_index = p->quota_to_dstIdx[newQuota];
	}
	else
	{
		p->dst_index = 0;
	}
#else
	// PER_BLOCK_QUOTA
#endif
#endif

#if	(EARLY_CORRECTION_MODE != 0)
	if ((p->utilization < UTILIZATION_EARLY_MODE) &&
		 (p->block_info[0] < (p->block_info[MAX_COPY_BACK]* EARLY_MODE_THRESHOLD)))
	{
		p->dst_index = 0;
		p->gc_mode = 2;	// early correction mode
	}
#endif

//		bdbm_msg("Alloc Src : blk : %lld, validpage :%lld, type: %lld", src_blk[0].block_no, (np->nr_subpages_per_block - src_blk[0].nr_invalid_subpages), src_blk[0].info);; 	
//		bdbm_page_ftl_print_blocks(bdi);

	bdbm_msg("GC %lld,V %lld,U %lld,M %lld, %lld", p->gc_count, p->src_valid_page_count, p->utilization, p->gc_mode,bdbm_abm_get_nr_free_blocks (p->bai)); 
}

void check_valid_bitmap(bdbm_abm_block_t* blk)
{
	uint64_t index;
	uint64_t expected_valid_page;
	uint64_t count = 0;
	
	expected_valid_page =	128*4 - blk->nr_invalid_subpages;

	for (index = 0; index < 16; index++)
	{
		uint8_t* bitmap = (uint8_t*)(blk->pst + index);
		uint64_t bit_idx;
		uint64_t byte_idx;
		for (byte_idx = 0; byte_idx < 8; byte_idx++)
		{
			for (bit_idx = 0; bit_idx < 8; bit_idx++)
			{
				if (bitmap[byte_idx] & (0x01 << bit_idx))
				{
					count++;
				}
			}
		}
	}

	if (count != expected_valid_page)
	{
		bdbm_msg("VALID_BITMAP_INCONSISTENCY: %lld, %lld, %lld, exp:%lld, real:%lld", blk->channel_no, blk->way_no, blk->block_no, expected_valid_page, count);
	}
	else
	{
//		bdbm_msg("  valid check ok :%lld", count);
	}
}

void __bdbm_page_ftl_flush_meta(bdbm_drv_info_t* bdi, uint64_t bGC_meta)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	uint64_t way, ch, unit, plane, subpage;
	bdbm_hlm_req_gc_t* hlm_gc = &p->gc_hlm; // used for host meta write.
	bdbm_llm_req_t* req = NULL;
	
	uint64_t dst_index = 0;
	if (p->dst_index != 0)
	{
		dst_index = 1;
	}
	
	for (way = 0; way < np->nr_units_per_channel; way++)
	{
		for (ch = 0; ch < np->nr_channels; ch++)
		{	
			uint64_t subpage_idx = 0;					
			unit = ch * np->nr_units_per_channel + way;

			// Write page
			/* build hlm_req_gc for writes */
			if (bGC_meta == 0)
			{
				// host active block meta write
				req = &hlm_gc->llm_reqs[p->host_meta_idx_start+unit];
				hlm_reqs_pool_reset_fmain (&req->fmain, BDBM_MAX_PAGES);
				hlm_reqs_pool_reset_logaddr (&req ->logaddr, BDBM_MAX_PAGES);

				// cumulative count is 0.
				bdbm_memset (req->fmain.kp_ptr[0], 0x0, np->nr_pages_per_block * sizeof(uint32_t));
			}
			else
			{
				// gc active block meta write
				req = &hlm_gc->llm_reqs[p->gc_meta_idx_start+unit];
			}
			
			req->req_type = REQTYPE_GC_WRITE; /* change to write */

			for (plane = 0; plane < np->nr_planes; plane++)
			{
				for (subpage = 0; subpage < np->nr_subpages_per_page; subpage++) 
				{
					/* move subpages that contain new data */
					req->fmain.kp_stt[subpage_idx] = KP_STT_DATA;

					((uint64_t*)req->foob.data)[subpage_idx] = 0xFFFFFF00 + unit;
					req->logaddr.lpa[subpage_idx] = ((uint64_t*)req->foob.data)[subpage_idx];

					subpage_idx++;
				}
			}

			req->dma = np->nr_subpages_per_page * np->nr_planes;
			req->ptr_hlm_req = (void*)hlm_gc;

			if (bGC_meta == 0)
			{
				if (bdbm_page_ftl_get_free_ppa(bdi, unit, &req->phyaddr) != 0) {
					bdbm_error ("bdbm_page_ftl_get_free_ppa failed");
					bdbm_bug_on (1);
				}
			}
			else
			{					
				if (bdbm_page_ftl_get_free_ppa_gc (bdi, unit, dst_index, &req->phyaddr) != 0) {
					bdbm_error ("bdbm_page_ftl_get_free_ppa_gc failed");
					bdbm_bug_on (1);
				}
			}

			// mapupdate is not needed.

			/* send write reqs to llm */
			hlm_gc->req_type = REQTYPE_GC_WRITE;
			hlm_gc->nr_llm_reqs++;

			if ((bdi->ptr_llm_inf->make_req (bdi, req)) != 0) {
				bdbm_error ("llm_make_req failed");
				bdbm_bug_on (1);
			}			
		}
	}

		
	if (bGC_meta == 0)
	{
		for (plane = 0; plane < np->nr_planes; plane++)
		{
#ifdef PER_PAGE_QUOTA
			bdbm_memset (p->cached_copyback_info + ((req->phyaddr.block_no + plane)* np->nr_pages_per_block), p->initial_quota, np->nr_pages_per_block);
#else		
			bdbm_memset (p->cached_copyback_info + ((req->phyaddr.block_no + plane)* np->nr_pages_per_block), 0x0, np->nr_pages_per_block);
#endif
			
		}
	}
	uint32_t* copyback_count = (uint32_t*)(req->fmain.kp_ptr[0]);
//	bdbm_msg(" meta flush : bGC_meta : %llu, copycount : %ld, %ld, %ld, %ld", bGC_meta, copyback_count[0], copyback_count[1], copyback_count[2], copyback_count[3]);

}

void __bdbm_page_ftl_load_meta(bdbm_drv_info_t* bdi)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	uint64_t unit, plane;
	bdbm_hlm_req_gc_t* hlm_gc = &p->gc_hlm; // used for host meta write.
	bdbm_abm_block_t* src_blk = NULL;

//	bdbm_msg("__bdbm_page_ftl_load_meta start: %lld,  %lld", hlm_gc->nr_llm_reqs, atomic64_read(&hlm_gc->nr_llm_reqs_done));

	for (plane = 0; plane < np->nr_planes; plane++)
	{
		for (unit = 0; unit < p->nr_punits; unit++)
		{
		  	uint64_t subPage; 
			src_blk = p->gc_src_bab[unit];
			bdbm_llm_req_t* req = &hlm_gc->llm_reqs[p->meta_load_idx_start + unit + plane*p->nr_punits]; // one request per plane	
			
			hlm_reqs_pool_reset_fmain (&req->fmain, BDBM_MAX_PAGES);
			hlm_reqs_pool_reset_logaddr (&req ->logaddr, BDBM_MAX_PAGES);
			
			for (subPage = 0; subPage < np->nr_subpages_per_page; subPage++)
			{
 				req->fmain.kp_stt[subPage] = KP_STT_DATA;
			}
			
			req->req_type = REQTYPE_GC_READ;
			req->dma = np->nr_subpages_per_page; // 0 - DMA bypass, 1 <= DMA

			req->phyaddr.channel_no = src_blk[plane].channel_no;
			req->phyaddr.way_no = src_blk[plane].way_no;
			req->phyaddr.block_no = src_blk[plane].block_no;
			req->phyaddr.page_no = np->nr_pages_per_block - 1;
			req->phyaddr.punit_id = BDBM_GET_PUNIT_ID (bdi, (&req->phyaddr));
			req->ptr_hlm_req = (void*)hlm_gc;
			req->ret = 0;
			
			/* send read reqs to llm */
			hlm_gc->req_type = REQTYPE_GC_READ;
			hlm_gc->nr_llm_reqs++;
			
			if ((bdi->ptr_llm_inf->make_req (bdi, req)) != 0) 
			{
				bdbm_error ("llm_make_req failed");
				bdbm_bug_on (1);
			}		
		}								
	}

	while (hlm_gc->nr_llm_reqs != atomic64_read(&hlm_gc->nr_llm_reqs_done))
	{
		// wait.
	}

#ifndef PER_PAGE_QUOTA		// per count management.
	//uint32_t* copyback_count = (uint32_t*)(hlm_gc->llm_reqs[p->meta_load_idx_start].fmain.kp_ptr[0]);
	//p->dst_index = copyback_count[0]+1;
	p->dst_index = p->cached_copyback_info[src_blk->block_no * np->nr_pages_per_block] + 1;
	if (p->dst_index == MAX_COPY_BACK)
	{
		p->dst_index = 0;
	}
#else
	uint32_t nCurQuota = p->cached_copyback_info[src_blk->block_no * np->nr_pages_per_block];

	if (nCurQuota >= p->blk_quota_data[src_blk->block_no].diminishing_quota)
	{
		p->dst_index = 1;
		p->new_quota = nCurQuota - p->blk_quota_data[src_blk->block_no].diminishing_quota;
	}	
	else
	{	
		p->dst_index = 0;
		p->new_quota = p->initial_quota;
	}	
#endif
	
//	bdbm_msg("__bdbm_page_ftl_load_meta end: reqs - %lld,  %lld, copyback count - %lld", hlm_gc->nr_llm_reqs, atomic64_read(&hlm_gc->nr_llm_reqs_done), p->dst_index);
}


uint64_t bdbm_page_ftl_gc_read_page(bdbm_drv_info_t* bdi, uint64_t unit, uint64_t plane, uint64_t average_valid)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	bdbm_hlm_req_gc_t* hlm_gc = &p->gc_hlm; // used for read
	bdbm_llm_req_t* req;

	bdbm_abm_block_t* src_blk;
	uint64_t page; 
	uint64_t idx = 0;
	uint64_t valid_subpage_count;
	uint64_t refresh = 0;
	uint64_t valid_count = 0;
	uint64_t bypassed_page = 0; 
	uint64_t page_offs = 0;

	uint64_t src_page = 0;
	uint64_t src_page_offs = 0;
	uint8_t* src_valid_bitmap;

	src_blk = p->gc_src_bab[unit];

	valid_subpage_count = np->nr_subpages_per_block - src_blk[plane].nr_invalid_subpages;

	if (p->dst_index == 0)
	{
		refresh = 1; // utilize internal copyback
	}
	
	p->src_unit_idx[plane][unit] = 0xFF;
	p->src_plane_idx[plane][unit] = 0xFF;
	
	req = &hlm_gc->llm_reqs[unit + plane*p->nr_punits]; // one request per plane	
	hlm_reqs_pool_reset_fmain (&req->fmain, BDBM_MAX_PAGES);
	hlm_reqs_pool_reset_logaddr (&req ->logaddr, BDBM_MAX_PAGES);
	
	if (valid_subpage_count == 0)
	{
		return 0;
	}
	
	p->src_unit_idx[plane][unit] = unit;
	p->src_plane_idx[plane][unit] = plane;
	
	for (page = p->gc_src_blk_offs[plane][unit]; page < np->nr_pages_per_block; page += 8)
	{
		uint64_t pst_idx = page / 8; // 64bit = 8bit x 8page consideration
		uint8_t* valid_bitmap;
		uint64_t is_break = 0;
		
		if (src_blk[plane].pst[pst_idx] == 0)
		{
			continue;
		}
		
		if (bypassed_page == 0)
		{		
			p->gc_src_blk_offs[plane][unit] = page;
		}

		valid_bitmap = (uint8_t*)(src_blk[plane].pst + pst_idx);
		
		for (page_offs = 0; page_offs < 8; page_offs++)
		{
			if (valid_bitmap[page_offs] != 0)
			{
				switch (valid_bitmap[page_offs])
				{
					case 15:					// 1111
						valid_count = 4;
						is_break = 1;
						src_page = page;
						src_page_offs = page_offs;
						src_valid_bitmap = valid_bitmap;						
						break;
						
					case 14: case 13: case 11: case 7:		// 1110, 1101, 1011, 0111
						valid_count = 3;
						is_break = 1;
						src_page = page;
						src_page_offs = page_offs;
						src_valid_bitmap = valid_bitmap;						
						break;
	
					case 12: case 10: case 9: case 6: case 5: case  3:	// 1100, 1010, 1001, 0110, 0101, 0011
						bypassed_page += 2;
						if (valid_count < 2)
						{
							valid_count = 2;
							src_page = page;
							src_page_offs = page_offs;
							src_valid_bitmap = valid_bitmap;
						}

						if (bypassed_page > 10)
						{
							is_break = 1;
						}
						break;
					
					case 8: case 4: case 2: case 1:			// 1000, 0100, 0010, 0001
						bypassed_page += 1;
						if (valid_count < 1)
						{
							valid_count = 1;
							src_page = page;
							src_page_offs = page_offs;
							src_valid_bitmap = valid_bitmap;
						}

						if (bypassed_page > 10)
						{
							is_break = 1;
						}					
						break;
				}
			}
			
			if (is_break != 0)
			{
				break;
			}
		}		
		
		if (is_break != 0)
		{
			break;
		}
	}

	if (valid_count != 0)
	{
		uint64_t subPage; 
	
//		bdbm_msg("	1 read: %lld,%lld, %lld, valid %lld, blkvalid %lld", plane, unit, src_page+src_page_offs, valid_count, valid_subpage_count);
		for (subPage = 0; subPage < np->nr_subpages_per_page; subPage++)
		{
			if (src_valid_bitmap[src_page_offs] & (0x01 << subPage))
			{
				req->fmain.kp_stt[subPage] = KP_STT_DATA;
			}
		}
	
		src_valid_bitmap[src_page_offs] = 0;
		src_blk[plane].nr_invalid_subpages += valid_count;
	}
	
	req->req_type = REQTYPE_GC_READ;
	req->dma = 0; // 0 - DMA bypass, 1 - DMA

#ifdef PER_PAGE_COPYBACK_MANAGEMENT
//	uint32_t* copyback_count = (uint32_t*)(hlm_gc->llm_reqs[p->meta_load_idx_start + unit].fmain.kp_ptr[0]);
//	p->dst_index = copyback_count[src_page + src_page_offs] + 1;

	refresh = 0;

	#ifndef PER_PAGE_QUOTA		// per count management.
	p->dst_index = p->cached_copyback_info[src_blk[plane].block_no * np->nr_pages_per_block + (src_page + src_page_offs)] + 1;

	if ( p->dst_index == MAX_COPY_BACK)
	{
		refresh = 1; // utilize int
		p->dst_index = 0;
	}
	#else
	uint32_t nCurQuota = p->cached_copyback_info[src_blk[plane].block_no * np->nr_pages_per_block + (src_page + src_page_offs)];

	if (nCurQuota >= p->blk_quota_data[src_blk->block_no].diminishing_quota)
	{
		p->dst_index = 1;			
		p->new_quota = nCurQuota - p->blk_quota_data[src_blk->block_no].diminishing_quota;
	}	
	else
	{
		refresh = 1; // utilize int
		p->dst_index = 0;
		p->new_quota = p->initial_quota;	
	}	
	#endif
#endif

	if (refresh == 0)
	{
		p->internal_copy_count += valid_count;
	}
	else
	{
		req->dma = valid_count;
		p->external_refresh_count += valid_count; // Error propagation prevention
	}
	
	req->phyaddr.channel_no = src_blk[plane].channel_no;
	req->phyaddr.way_no = src_blk[plane].way_no;
	req->phyaddr.block_no = src_blk[plane].block_no;
	req->phyaddr.page_no = src_page + src_page_offs;
	req->phyaddr.punit_id = BDBM_GET_PUNIT_ID (bdi, (&req->phyaddr));
	req->ptr_hlm_req = (void*)hlm_gc;
	req->ret = 0;
	
	/* send read reqs to llm */
	hlm_gc->req_type = REQTYPE_GC_READ;
	hlm_gc->nr_llm_reqs++;
	
	if ((bdi->ptr_llm_inf->make_req (bdi, req)) != 0) 
	{
		bdbm_error ("llm_make_req failed");
		bdbm_bug_on (1);
	}		

	if (valid_count == 0)
	{
		bdbm_msg("valid page : %lld, plane: %lld, unit:%lld, page:%lld, pageoff:%lld,", valid_subpage_count, plane, unit, page, page_offs);
		while(1);
	}

	return valid_subpage_count - valid_count;
}



uint64_t bdbm_page_ftl_gc_read_partial_page(bdbm_drv_info_t* bdi, uint64_t unit, uint64_t plane,uint64_t average_valid)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	bdbm_hlm_req_gc_t* hlm_gc = &p->gc_hlm; // used for read
	bdbm_llm_req_t* req = NULL;

	bdbm_abm_block_t* src_blk;
	uint64_t page; 
	uint64_t valid_subpage_count;
	uint64_t page_offs = 0;
	uint64_t threshold = np->nr_subpages_per_page;
	uint64_t forced_read = 0;
	uint64_t required_page = np->nr_subpages_per_page;
	uint64_t check_count = 0;

	src_blk = p->gc_src_bab[unit];

	valid_subpage_count = np->nr_subpages_per_block - src_blk[plane].nr_invalid_subpages;
	if ((average_valid > valid_subpage_count + threshold) || (valid_subpage_count == 0))
	{
		// doesn't need to read
		return required_page;
	}
	else if (average_valid + threshold <= valid_subpage_count)
	{
		// read as much as possible
		forced_read = 1;			
	}
	
	uint64_t req_idx = p->partial_tail; 
	if (req_idx == p->partial_read_end)
	{
		p->partial_tail = p->partial_read_start;		
		req_idx = p->partial_tail; 
	}
	
	for (page = p->gc_src_blk_offs[plane][unit]; page < np->nr_pages_per_block; page += 8)
	{
		uint64_t pst_idx = page / 8; // 64bit = 8bit x 8page consideration
		uint8_t* valid_bitmap;
		uint64_t found = 0; 
		
		if (src_blk[plane].pst[pst_idx] == 0)
		{
			continue;
		}

		if (check_count == 0)
		{		
			p->gc_src_blk_offs[plane][unit] = page;
		}

		valid_bitmap = (uint8_t*)(src_blk[plane].pst + pst_idx);					
		for (page_offs = 0; page_offs < 8; page_offs++)
		{
			if (check_count > 2)
			{
				return required_page;
			}

			if (valid_bitmap[page_offs] != 0)
			{
				switch (valid_bitmap[page_offs])
				{
					case 15:					// 1111
						required_page = 0;

						if (forced_read != 0)
						{
							found = 1;
						}
						else
						{
							if ((average_valid == np->nr_subpages_per_page) && (valid_subpage_count > np->nr_subpages_per_page))
							{
								found = 1;
							}
							else
							{														
								check_count++;
								//return required_page;
							}
						}
						break;

					case 14: case 13: case 11: case 7: 		// 1110, 1101, 1011, 0111		
						if (required_page > 1)
						{
							required_page = 1;
						}

						if ((forced_read != 0) || ((p->required_subpage_count > p->buffered_subpage_count) && (valid_subpage_count > 3)))
						{
							found = 1;
						}
						else					
						{
							check_count++;
							//return required_page;
						}
						break;

					case 12: case 10: case 9: case 6: case 5: case 3:	// 1100, 1010, 1001, 0110, 0101, 0011
						if (required_page > 2)
						{
							required_page = 2;
						}
						found = 1;
						break;

					case 8: case 4: case 2: case 1:			// 1000, 0100, 0010, 0001
						if (required_page > 3)
						{
							required_page = 3;
						}
						found = 1;
						break;
				}					
			}
			
			if (found != 0)
			{
				break;
			}
		}
		
		if (found != 0)
		{
			uint64_t subPage; 
			uint64_t valid_count = 0;

			req = &hlm_gc->llm_reqs[req_idx]; // one request per plane
			hlm_reqs_pool_reset_fmain (&req->fmain, BDBM_MAX_PAGES);
			hlm_reqs_pool_reset_logaddr (&req ->logaddr, BDBM_MAX_PAGES);
	
			for (subPage = 0; subPage < np->nr_subpages_per_page; subPage++)
			{
				if (valid_bitmap[page_offs] & (0x01 << subPage))
				{
					req->fmain.kp_stt[subPage] = KP_STT_DATA;\
					valid_count++;
				}
			}

//			bdbm_msg("  2 read: %lld,%lld, %lld, valid %lld, blkvalid %lld", plane, unit, page+page_offs, valid_count, valid_subpage_count);

			valid_bitmap[page_offs] = 0;
			src_blk[plane].nr_invalid_subpages += valid_count;	// read out only 1 page
			if (forced_read != 0)
			{
				p->external_die_difference_count += valid_count;
			}
			else
			{
				p->external_partial_valid_count += valid_count; // Dieº° ºÒ±ÕÇü.
			}

			req->dma = valid_count; // 0 - DMA bypass, 1 - DMA
			p->buffered_subpage_count += valid_count;
			
			break;
		}
	}
	
	if (req != NULL)
	{
		req->req_type = REQTYPE_GC_READ;
		req->phyaddr.channel_no = src_blk[plane].channel_no;
		req->phyaddr.way_no = src_blk[plane].way_no;
		req->phyaddr.block_no = src_blk[plane].block_no;
		req->phyaddr.page_no = page + page_offs;
		req->phyaddr.punit_id = BDBM_GET_PUNIT_ID (bdi, (&req->phyaddr));
		req->ptr_hlm_req = (void*)hlm_gc;
		req->ret = 0;
		
		/* send read reqs to llm */
		hlm_gc->req_type = REQTYPE_GC_READ;
		hlm_gc->nr_llm_reqs++;
		
		if ((bdi->ptr_llm_inf->make_req (bdi, req)) != 0) 
		{
			bdbm_error ("llm_make_req failed");
			bdbm_bug_on (1);
		}		

		p->partial_tail++;
	}

	return required_page;
}

uint32_t bdbm_page_ftl_gc_read_state_adv(bdbm_drv_info_t* bdi)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	uint64_t unit, plane;
	uint64_t average_valid;
	uint64_t round;

	// 1. Check Src Validpagecount
	if (p->src_valid != 0)
	{ 
		if (p->src_valid_page_count == 0)
		{
			bdbm_page_ftl_restore_srcblk(bdi);
			p->src_valid = 0;
			p->gc_count++;
			return 0;
		}
	}
	else 
	{
		bdbm_page_ftl_alloc_srcblk(bdi);
		p->src_valid = 1;

		if (p->src_valid_page_count == 0)
		{
			// special case handling
			return 0;
		}	

#ifdef PER_PAGE_COPYBACK_MANAGEMENT
		__bdbm_page_ftl_load_meta(bdi);
#endif
	}

	average_valid = ((p->src_valid_page_count + p->nr_punits - 1) / p->nr_punits) / np->nr_planes;

	// 2. read partial/.. page.
	uint64_t min_required_subpage_count = 0x7FFF;
	
//	bdbm_msg(" readpartial : %lld, %lld, average:%lld", p->buffered_subpage_count, p->required_subpage_count, average_valid);
	for (round = 0; round < np->nr_subpages_per_page; round++)
	{
		uint64_t required_page_count = 0;

		if (p->buffered_subpage_count >  p->required_subpage_count)
		{
			break;
		}

		for (plane = 0; plane < np->nr_planes; plane++)
		{
			for (unit = 0; unit < p->nr_punits; unit++)
			{
				required_page_count += bdbm_page_ftl_gc_read_partial_page(bdi, unit, plane, average_valid);
			}
		}
		
//`		bdbm_msg(" readpartial rnd:%lld, %lld, %lld", round, p->buffered_subpage_count, required_page_count);

		if (min_required_subpage_count > required_page_count)
		{
			min_required_subpage_count = required_page_count;
			p->required_subpage_count = min_required_subpage_count;
		}
		
		if (p->buffered_subpage_count >=  min_required_subpage_count)
		{
			break;
		}
	}

	// 3. read nr_punits page - as large as possible
	uint64_t valid_subpage = 0;
	for (plane = 0; plane < np->nr_planes; plane++)
	{
		for (unit = 0; unit < p->nr_punits; unit++)
		{
			valid_subpage += bdbm_page_ftl_gc_read_page(bdi, unit, plane, average_valid);
		}								
	}

//	bdbm_msg(" valid diff, %lld, %lld, b %lld, r %lld", p->src_valid_page_count, valid_subpage, p->buffered_subpage_count, p->required_subpage_count);
	p->src_valid_page_count = valid_subpage;

	return 1;
}

uint32_t bdbm_page_ftl_gc_write_state_adv(bdbm_drv_info_t* bdi)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	bdbm_hlm_req_gc_t* hlm_gc = &p->gc_hlm; // used for read
	bdbm_hlm_req_gc_t* hlm_gc_w = &p->gc_hlm_w; // used for write.
	uint64_t ch, way, subPage, unit, plane;
	bdbm_llm_req_t* req = NULL;
	bdbm_abm_block_t* src_blk = NULL;
	uint64_t dst_index = p->dst_index; // default

#ifdef PER_PAGE_COPYBACK_MANAGEMENT
	if (dst_index != 0)
	{
		dst_index = 1;
	}
#endif		

	// copy information from read to write hlm...l
//	bdbm_msg("before head :%lld %lld", p->partial_head, p->buffered_subpage_count);
	uint64_t valid_page_count = hlm_reqs_pool_compaction(hlm_gc_w, hlm_gc, np, p->dst_offset, &(p->partial_head), p->partial_tail, &p->buffered_subpage_count);
//	bdbm_msg("after head :%lld %lld", p->partial_head, p->buffered_subpage_count);
	
	for (way = 0; way < np->nr_units_per_channel; way++)
	{
		for (ch = 0; ch < np->nr_channels; ch++)
		{	
			uint64_t subPage_idx = 0;					
			unit = ch * np->nr_units_per_channel + way;

			// Write page
			/* build hlm_req_gc for writes */
			req = &hlm_gc_w->llm_reqs[p->dst_offset+unit];
			req->req_type = REQTYPE_GC_WRITE; /* change to write */

			for (plane = 0; plane < np->nr_planes; plane++)
			{
				for (subPage = 0; subPage < np->nr_subpages_per_page; subPage++) 
				{
					/* move subpages that contain new data */
					if (req->fmain.kp_stt[subPage_idx] == KP_STT_DATA) 
					{
						bdbm_phyaddr_t phyaddr; 
						uint64_t sp_offs;					

						req->logaddr.lpa[subPage_idx] = ((uint64_t*)req->foob.data)[subPage_idx];
					}
					else if (req->fmain.kp_stt[subPage_idx] == KP_STT_HOLE) 
					{
						((uint64_t*)req->foob.data)[subPage_idx] = -1;
						req->logaddr.lpa[subPage_idx] = -1;
					} 
					else 
					{
						bdbm_error (" invalid else");
						bdbm_bug_on (1);
					}

					subPage_idx++;
				}
			}
	
			req->ptr_hlm_req = (void*)hlm_gc_w;
		
			if (bdbm_page_ftl_get_free_ppa_gc (bdi, unit, dst_index, &req->phyaddr) != 0) {
				bdbm_error ("bdbm_page_ftl_get_free_ppa failed");
				bdbm_bug_on (1);
			}

			if (bdbm_page_ftl_map_lpa_to_ppa (bdi, &req->logaddr, &req->phyaddr, 1) != 0) 
			{
				bdbm_error ("bdbm_page_ftl_map_lpa_to_ppa failed");
				bdbm_bug_on (1);
			}

			/* send write reqs to llm */
			hlm_gc_w->req_type = REQTYPE_GC_WRITE;
			hlm_gc_w->nr_llm_reqs++;

			if ((bdi->ptr_llm_inf->make_req (bdi, req)) != 0) {
				bdbm_error ("llm_make_req failed");
				bdbm_bug_on (1);
			}


			uint64_t bypass = np->nr_subpages_per_page * np->nr_planes - req->dma;
			p->bypass_write += bypass;
			p->dma_write += req->dma;

#ifdef PER_PAGE_COPYBACK_MANAGEMENT
			// update accumulated copyback count 			
			//uint32_t* copyback_count = (uint32_t*)(hlm_gc->llm_reqs[p->gc_meta_idx_start + unit].fmain.kp_ptr[0]);
			//copyback_count[req->phyaddr.page_no] = p->dst_index;

		#ifndef PER_PAGE_QUOTA
			for (plane = 0; plane < np->nr_planes; plane++)
			{
				p->cached_copyback_info[(req->phyaddr.block_no + plane) * np->nr_pages_per_block + req->phyaddr.page_no] = p->dst_index;
			}
		#else
			for (plane = 0; plane < np->nr_planes; plane++)
			{
				p->cached_copyback_info[(req->phyaddr.block_no + plane) * np->nr_pages_per_block + req->phyaddr.page_no] = p->new_quota;
			}
		#endif


			if (unit == 0)
			{
			//	bdbm_msg("	write_page : %lld,  copyback count : %lld", req->phyaddr.page_no, p->dst_index);
			}
#endif	
		}
	}

	p->gc_copy_count += p->gc_subpages_move_unit;
	p->dst_offset = req->phyaddr.page_no * p->nr_punits;
	
#ifdef PER_PAGE_COPYBACK_MANAGEMENT
	if (req->phyaddr.page_no == np->nr_pages_per_block - 2)
	{
		// need to flush copyback meta.
		__bdbm_page_ftl_flush_meta(bdi,/*bGC_meta*/ 1);
	}
#endif
	
	//bdbm_msg("write page off : %lld, valid page count :%lld", req->phyaddr.page_no,p->src_valid_page_count);

	return 0;
}



 
uint32_t bdbm_page_ftl_gc_write_state_default(bdbm_drv_info_t* bdi)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	bdbm_hlm_req_gc_t* hlm_gc = &p->gc_hlm; // used for read
	bdbm_hlm_req_gc_t* hlm_gc_w = &p->gc_hlm_w; // used for write.
	uint64_t ch, way, subPage, unit;
	bdbm_llm_req_t* req = NULL;
	bdbm_abm_block_t* src_blk = NULL;

	// copy information from read to write hlm...
	hlm_reqs_pool_copy (hlm_gc_w, hlm_gc, np, p->dst_offset);

	for (way = 0; way < np->nr_units_per_channel; way++)
	{
		for (ch = 0; ch < np->nr_channels; ch++)
		{
			uint64_t src_unit;
			uint64_t write_bypass = 1; 
			uint64_t dst_index = 0; // default			
			uint64_t write_dma = 1; // need to execute DMA to NAND.		
	
			unit = ch * np->nr_units_per_channel + way;
			src_unit = p->src_unit_idx[0][unit];	
			src_blk = p->gc_src_bab[src_unit];

			// using. src->copy_count info, src/dst plane information.
			if (unit == src_unit)
			{
				dst_index = src_blk->copy_count + 1;
				if (dst_index < MAX_COPY_BACK)
				{
					write_dma = 0; // utilize internal copyback
				}
				else
				{
					dst_index = 0;
				}
			}

			// Write page
			/* build hlm_req_gc for writes */
			req = &hlm_gc_w->llm_reqs[p->dst_offset+unit];
			req->req_type = REQTYPE_GC_WRITE; /* change to write */
			req->dma = write_dma;			

			for (subPage = 0; subPage < np->nr_subpages_per_page; subPage++) 
			{
				/* move subpages that contain new data */
				if (req->fmain.kp_stt[subPage] == KP_STT_DATA) 
				{
					bdbm_phyaddr_t phyaddr; 
					uint64_t sp_offs;					

					req->logaddr.lpa[subPage] = ((uint64_t*)req->foob.data)[subPage];

					bdbm_page_ftl_get_ppa(bdi, req->logaddr.lpa[subPage],&phyaddr,&sp_offs); // previous map check for overwrite during G.C
					if ( (phyaddr.block_no == src_blk->block_no) && (phyaddr.channel_no == src_blk->channel_no) && (phyaddr.way_no == src_blk->way_no))
					{
						write_bypass = 0; // TODO>. VALID check
					} 
					else
					{
						bdbm_msg (" Host Update during G.C");
						bdbm_msg (" lpa : %lld, org ch: %lld,%lld,%lld,%d", req->logaddr.lpa[subPage], src_blk->channel_no, src_blk->way_no, src_blk->block_no, src_blk->info);
						bdbm_msg (" 			new ch: %lld,%lld,%lld, page: %lld", phyaddr.channel_no, phyaddr.way_no, phyaddr.block_no, phyaddr.page_no);

						bdbm_page_ftl_print_blocks(bdi);
					}
				}
				else if (req->fmain.kp_stt[subPage] == KP_STT_HOLE) 
				{
					((uint64_t*)req->foob.data)[subPage] = -1;
					req->logaddr.lpa[subPage] = -1;
				} 
				else 
				{
					bdbm_error (" invalid else");
					bdbm_bug_on (1);
				}
			}
	
			req->ptr_hlm_req = (void*)hlm_gc_w;
		
			if (bdbm_page_ftl_get_free_ppa_gc (bdi, unit, dst_index, &req->phyaddr) != 0) {
				bdbm_error ("bdbm_page_ftl_get_free_ppa failed");
				bdbm_bug_on (1);
			}

			if (bdbm_page_ftl_map_lpa_to_ppa (bdi, &req->logaddr, &req->phyaddr, 1) != 0) 
			{
				bdbm_error ("bdbm_page_ftl_map_lpa_to_ppa failed");
				bdbm_bug_on (1);
			}
		
			/* send write reqs to llm */
			hlm_gc_w->req_type = REQTYPE_GC_WRITE;
			hlm_gc_w->nr_llm_reqs++;

			if ((bdi->ptr_llm_inf->make_req (bdi, req)) != 0) {
				bdbm_error ("llm_make_req failed");
				bdbm_bug_on (1);
			}
		}
	}

	p->gc_copy_count += p->nr_punits; 

	if (p->src_valid_page_count > p->nr_punits)
	{
		p->src_valid_page_count -= p->nr_punits; 
	}
	else
	{
		p->src_valid_page_count = 0;
	}

	p->dst_offset = req->phyaddr.page_no * p->nr_punits;
		
//	bdbm_msg("	write page : %lld", p->src_valid_page_count);
 
	return 0;
}

uint32_t bdbm_page_ftl_do_gc (bdbm_drv_info_t* bdi, int64_t utilization)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_hlm_req_gc_t* hlm_gc = &p->gc_hlm; // used for read
	bdbm_hlm_req_gc_t* hlm_gc_w = &p->gc_hlm_w; // used for write.
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS(bdi);

	static uint32_t state = 0;
	p->utilization = utilization;	
	p->nop_count++;
	if ((p->nop_count % 3000000) == 0)
	{
		if (p->nop_count < 30000000)
		{
			bdbm_msg("nop count : %lld, read pending : %lld", p->nop_count, hlm_gc->nr_llm_reqs - atomic64_read(&hlm_gc->nr_llm_reqs_done));
		}
		else
		{
			bdbm_bug_on(1);
		}
	}

#if 0
	if ((state == 0) && (hlm_gc_w->nr_llm_reqs >= atomic64_read(&hlm_gc_w->nr_llm_reqs_done)+p->nr_punits))
	{
		// write should not be pended too much
		return 0;
	}

	if ((state == 1) && (hlm_gc->nr_llm_reqs > atomic64_read(&hlm_gc->nr_llm_reqs_done)))
	{
		// read should be finished
		return 1;
	}
#else
	if (state == 0) 
	{
		if (bdi->ptr_llm_inf->get_queuing_count(bdi) >= np->nr_units_per_ssd)
		{
			// write should not be pended too much
			return 0;
		}
	}

	if (state == 1) 
	{
		if ((hlm_gc->nr_llm_reqs > atomic64_read(&hlm_gc->nr_llm_reqs_done)) || (bdi->ptr_llm_inf->get_queuing_count(bdi) >= np->nr_units_per_ssd))
		{
			// read should be finished
			return 1;
		}
	}
#endif
	p->nop_count = 0;
	
	if (state == 0)
	{
		//bdbm_msg(" __Read_State");	
		state = bdbm_page_ftl_gc_read_state_adv(bdi);
	}
	else
	{
		//bdbm_msg(" __Write_State : %lld, %lld", p->token_count, p->token_mode);	
		state = bdbm_page_ftl_gc_write_state_adv(bdi);
		p->token_count += p->generated_token;
	}
	
	return state;
}


/* for snapshot */
uint32_t bdbm_page_ftl_load (bdbm_drv_info_t* bdi, const char* fn)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	bdbm_page_mapping_entry_t* me;
	bdbm_file_t fp = 0;
	uint64_t i, pos = 0;

	/* step1: load abm */
	if (bdbm_abm_load (p->bai, "/usr/share/bdbm_drv/abm.dat") != 0) {
		bdbm_error ("bdbm_abm_load failed");
		return 1;
	}

	/* step2: load mapping table */
	if ((fp = bdbm_fopen (fn, O_RDWR, 0777)) == 0) {
		bdbm_error ("bdbm_fopen failed");
		return 1;
	}

	me = p->ptr_mapping_table;
	for (i = 0; i < np->nr_subpages_per_ssd; i++) {
		pos += bdbm_fread (fp, pos, (uint8_t*)&me[i], sizeof (bdbm_page_mapping_entry_t));
		if (me[i].status != PFTL_PAGE_NOT_ALLOCATED &&
			me[i].status != PFTL_PAGE_VALID &&
			me[i].status != PFTL_PAGE_INVALID &&
			me[i].status != PFTL_PAGE_INVALID_ADDR) {
			bdbm_msg ("snapshot: invalid status = %u", me[i].status);
		}
	}

	/* step3: get active blocks */
	if (__bdbm_page_ftl_get_active_blocks (np, p->bai, p->ac_bab) != 0) {
		bdbm_error ("__bdbm_page_ftl_get_active_blocks failed");
		bdbm_fclose (fp);
		return 1;
	}
	p->curr_puid = 0;
	p->curr_page_ofs = 0;

	bdbm_fclose (fp);

	return 0;
}

uint32_t bdbm_page_ftl_store (bdbm_drv_info_t* bdi, const char* fn)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	bdbm_page_mapping_entry_t* me;
	bdbm_abm_block_t* b = NULL;
	bdbm_file_t fp = 0;
	uint64_t pos = 0;
	uint64_t i, j, k;
	uint32_t ret;

	/* step1: make active blocks invalid (it's ugly!!!) */
	if ((fp = bdbm_fopen (fn, O_CREAT | O_WRONLY, 0777)) == 0) {
		bdbm_error ("bdbm_fopen failed");
		return 1;
	}

	while (1) {
		/* get the channel & chip numbers */
		i = p->curr_puid % np->nr_channels;
		j = p->curr_puid / np->nr_channels;

		/* get the physical offset of the active blocks */
		b = p->ac_bab[i*np->nr_units_per_channel + j];

		/* invalidate remaining pages */
		for (k = 0; k < np->nr_subpages_per_page; k++) {
			bdbm_abm_invalidate_page (
				p->bai, 
				b->channel_no, 
				b->way_no, 
				b->block_no, 
				p->curr_page_ofs, 
				k);
		}
		bdbm_bug_on (b->channel_no != i);
		bdbm_bug_on (b->way_no != j);

		/* go to the next parallel unit */
		if ((p->curr_puid + 1) == p->nr_punits) {
			p->curr_puid = 0;
			p->curr_page_ofs++;	/* go to the next page */

			/* see if there are sufficient free pages or not */
			if (p->curr_page_ofs == np->nr_pages_per_block) {
				p->curr_page_ofs = 0;
				break;
			}
		} else {
			p->curr_puid++;
		}
	}

	/* step2: store mapping table */
	me = p->ptr_mapping_table;
	for (i = 0; i < np->nr_subpages_per_ssd; i++) {
		pos += bdbm_fwrite (fp, pos, (uint8_t*)&me[i], sizeof (bdbm_page_mapping_entry_t));
	}
	bdbm_fsync (fp);
	bdbm_fclose (fp);

	/* step3: store abm */
	ret = bdbm_abm_store (p->bai, "/usr/share/bdbm_drv/abm.dat");

	return ret;
}

void __bdbm_page_badblock_scan_eraseblks (
	bdbm_drv_info_t* bdi,
	uint64_t block_no)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	bdbm_hlm_req_gc_t* hlm_gc = &p->gc_hlm;
	uint64_t i, j;

	/* setup blocks to erase */
	bdbm_memset (p->gc_src_bab, 0x00, sizeof (bdbm_abm_block_t*) * p->nr_punits);
	for (i = 0; i < np->nr_channels; i++) {
		for (j = 0; j < np->nr_units_per_channel; j++) {
			bdbm_abm_block_t* b = NULL;
			bdbm_llm_req_t* r = NULL;
			uint64_t punit_id = i*np->nr_units_per_channel+j;

			if ((b = bdbm_abm_get_block (p->bai, i, j, block_no)) == NULL) {
				bdbm_error ("oops! bdbm_abm_get_block failed");
				bdbm_bug_on (1);
			}
			p->gc_src_bab[punit_id] = b;

			r = &hlm_gc->llm_reqs[punit_id];
			r->req_type = REQTYPE_GC_ERASE;
			r->logaddr.lpa[0] = -1ULL; /* lpa is not available now */
			r->phyaddr.channel_no = b->channel_no;
			r->phyaddr.way_no = b->way_no;
			r->phyaddr.block_no = b->block_no;
			r->phyaddr.page_no = 0;
			r->phyaddr.punit_id = BDBM_GET_PUNIT_ID (bdi, (&r->phyaddr));
			r->ptr_hlm_req = (void*)hlm_gc;
			r->ret = 0;
		}
	}

	/* send erase reqs to llm */
	hlm_gc->req_type = REQTYPE_GC_ERASE;
	hlm_gc->nr_llm_reqs = p->nr_punits;
	atomic64_set (&hlm_gc->nr_llm_reqs_done, 0);
	bdbm_sema_lock (&hlm_gc->done);
	for (i = 0; i < p->nr_punits; i++) {
		if ((bdi->ptr_llm_inf->make_req (bdi, &hlm_gc->llm_reqs[i])) != 0) {
			bdbm_error ("llm_make_req failed");
			bdbm_bug_on (1);
		}
	}
	bdbm_sema_lock (&hlm_gc->done);
	bdbm_sema_unlock (&hlm_gc->done);

	for (i = 0; i < p->nr_punits; i++) {
		uint8_t ret = 0;
		bdbm_abm_block_t* b = p->gc_src_bab[i];

		if (hlm_gc->llm_reqs[i].ret != 0) {
			ret = 1; /* bad block */
		}

		bdbm_abm_erase_block (p->bai, b->channel_no, b->way_no, b->block_no, ret);
	}

	/* measure gc elapsed time */
}

static void __bdbm_page_mark_it_dead (
	bdbm_drv_info_t* bdi,
	uint64_t block_no)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	int i, j;

	for (i = 0; i < np->nr_channels; i++) {
		for (j = 0; j < np->nr_units_per_channel; j++) {
			bdbm_abm_block_t* b = NULL;

			if ((b = bdbm_abm_get_block (p->bai, i, j, block_no)) == NULL) {
				bdbm_error ("oops! bdbm_abm_get_block failed");
				bdbm_bug_on (1);
			}

			bdbm_abm_set_to_dirty_block (p->bai, i, j, block_no);
		}
	}
}

uint32_t bdbm_page_badblock_scan (bdbm_drv_info_t* bdi)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	bdbm_page_mapping_entry_t* me = NULL;
	uint64_t i = 0;
	uint32_t ret = 0;

	bdbm_msg ("[WARNING] 'bdbm_page_badblock_scan' is called! All of the flash blocks will be erased!!!");

	/* step1: reset the page-level mapping table */
	bdbm_msg ("step1: reset the page-level mapping table");
	me = p->ptr_mapping_table;
	for (i = 0; i < np->nr_subpages_per_ssd; i++) {
		me[i].status = PFTL_PAGE_NOT_ALLOCATED;
		me[i].phyaddr.channel_no = PFTL_PAGE_INVALID_ADDR;
		me[i].phyaddr.way_no = PFTL_PAGE_INVALID_ADDR;
		me[i].phyaddr.block_no = PFTL_PAGE_INVALID_ADDR;
		me[i].phyaddr.page_no = PFTL_PAGE_INVALID_ADDR;
		me[i].sp_off = -1;
	}

	/* step2: erase all the blocks */
	bdi->ptr_llm_inf->flush (bdi);
	for (i = 0; i < np->nr_blocks_per_die; i++) {
		__bdbm_page_badblock_scan_eraseblks (bdi, i);
	}

	/* step3: store abm */
	if ((ret = bdbm_abm_store (p->bai, "/usr/share/bdbm_drv/abm.dat"))) {
		bdbm_error ("bdbm_abm_store failed");
		return 1;
	}

	/* step4: get active blocks */
	bdbm_msg ("step2: get active blocks");
	if (__bdbm_page_ftl_get_active_blocks (np, p->bai, p->ac_bab) != 0) {
		bdbm_error ("__bdbm_page_ftl_get_active_blocks failed");
		return 1;
	}
	p->curr_puid = 0;
	p->curr_page_ofs = 0;

	bdbm_msg ("done");
	 
	return 0;

#if 0
	/* TEMP: on-demand format */
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	bdbm_page_mapping_entry_t* me = NULL;
	uint64_t i = 0;
	uint32_t ret = 0;
	uint32_t erased_blocks = 0;

	bdbm_msg ("[WARNING] 'bdbm_page_badblock_scan' is called! All of the flash blocks will be dirty!!!");

	/* step1: reset the page-level mapping table */
	bdbm_msg ("step1: reset the page-level mapping table");
	me = p->ptr_mapping_table;
	for (i = 0; i < np->nr_pages_per_ssd; i++) {
		me[i].status = PFTL_PAGE_NOT_ALLOCATED;
		me[i].phyaddr.channel_no = PFTL_PAGE_INVALID_ADDR;
		me[i].phyaddr.way_no = PFTL_PAGE_INVALID_ADDR;
		me[i].phyaddr.block_no = PFTL_PAGE_INVALID_ADDR;
		me[i].phyaddr.page_no = PFTL_PAGE_INVALID_ADDR;
	}

	/* step2: erase all the blocks */
	bdi->ptr_llm_inf->flush (bdi);
	for (i = 0; i < np->nr_blocks_per_die; i++) {
		if (erased_blocks <= p->nr_punits)
			__bdbm_page_badblock_scan_eraseblks (bdi, i);
		else 
			__bdbm_page_mark_it_dead (bdi, i);
		erased_blocks += np->nr_channels;
	}

	/* step3: store abm */
	if ((ret = bdbm_abm_store (p->bai, "/usr/share/bdbm_drv/abm.dat"))) {
		bdbm_error ("bdbm_abm_store failed");
		return 1;
	}

	/* step4: get active blocks */
	bdbm_msg ("step2: get active blocks");
	if (__bdbm_page_ftl_get_active_blocks (np, p->bai, p->ac_bab) != 0) {
		bdbm_error ("__bdbm_page_ftl_get_active_blocks failed");
		return 1;
	}
	p->curr_puid = 0;
	p->curr_page_ofs = 0;

	bdbm_msg ("[summary] Total: %llu, Free: %llu, Clean: %llu, Dirty: %llu",
		bdbm_abm_get_nr_total_blocks (p->bai),
		bdbm_abm_get_nr_free_blocks (p->bai),
		bdbm_abm_get_nr_clean_blocks (p->bai),
		bdbm_abm_get_nr_dirty_blocks (p->bai)
	);
#endif
	bdbm_msg ("done");
	 
	return 0;

}

uint32_t bdbm_page_ftl_get_token (bdbm_drv_info_t* bdi)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	uint32_t token = 1024;

	if (p->token_mode)
	{
		token = p->token_count;
	}

	return token;
}

void bdbm_page_ftl_consume_token (bdbm_drv_info_t* bdi, uint32_t used_token)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;

	if (p->token_mode)
	{
		if (p->token_count > used_token)
		{
			p->token_count -= used_token; 
		}
		else
		{
			p->token_count = 0;
		}			
	}
}

void bdbm_page_ftl_flush_meta(bdbm_drv_info_t* bdi)
{	
	__bdbm_page_ftl_flush_meta(bdi, /*bGC_meta*/ 0);
}
