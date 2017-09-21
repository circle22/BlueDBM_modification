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
	bdbm_abm_info_t* bai;
	bdbm_page_mapping_entry_t* ptr_mapping_table;
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

	uint64_t* gc_src_blk_offs; // for each ch x way.
	uint64_t* gc_dst_blk_offs[MAX_COPY_BACK]; // for each ch x way x copybackCount	

	bdbm_hlm_req_gc_t gc_hlm;
	bdbm_hlm_req_gc_t gc_hlm_w;

	/* for bad-block scanning */
	bdbm_sema_t badblk;

	// infomative data.	
	uint64_t valid_page_count[8][8];
	uint64_t validpage_balancing;
	uint64_t max_plane;
	uint64_t target_plane;
	uint64_t substituted;
	uint64_t average_valid_count;

	uint64_t cumulative_adjust_count;
	uint64_t gc_copy_count;
	uint64_t nop_count;

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
		me[loop].phyaddr.chip_no = PFTL_PAGE_INVALID_ADDR;
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

	for (i = 0; i < np->nr_channels; i++) {
		for (j = 0; j < np->nr_chips_per_channel; j++) {
			if (bai->anr_free_blks[i][j] < 2)
			{
				// Need to go GC.
				return 1;
			}
		}
	}


	/* get a set of free blocks for active blocks */
	for (i = 0; i < np->nr_channels; i++) {
		for (j = 0; j < np->nr_chips_per_channel; j++) {
			/* prepare & commit free blocks */
			if ((*bab = bdbm_abm_get_free_block_prepare (bai, i, j))) {
				bdbm_abm_get_free_block_commit (bai, *bab);
				/*bdbm_msg ("active blk = %p", *bab);*/
				
				(*bab)->info = 10;// active	
				//if ( i==0 && j==0)
				{
				//	bdbm_msg("A: %lld,%lld,%lld", (*bab)->channel_no, (*bab)->chip_no, (*bab)->block_no);
				}
				bab++;
			} else {
				bdbm_error ("bdbm_abm_get_free_block_prepare failed");
				return 1;
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

	nr_punits = np->nr_chips_per_channel * np->nr_channels;

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
	uint32_t i = 0, j = 0;
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
	p->nr_punits = np->nr_chips_per_channel * np->nr_channels;
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
	}

	// src block offset.
	if ((p->gc_src_blk_offs = (uint64_t*)bdbm_zmalloc
			(sizeof(uint64_t) * p->nr_punits)) == NULL)
	{
		bdbm_error ("bdbm_zmalloc failed");
		bdbm_page_ftl_destroy (bdi);		
	}
	
	for (i = 0; i < p->nr_punits; i++)
	{
		p->gc_src_blk_offs[i] = np->nr_pages_per_block;

		for (j = 0; j < MAX_COPY_BACK; j++)
		{
			p->gc_dst_blk_offs[j][i] = np->nr_pages_per_block;	
		}
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

	for (i = 0; i < np->nr_channels; i++)
	{
		for (j =  0; j < np->nr_chips_per_channel; j++)
		{
			p->valid_page_count[i][j] = 0;
		}
	}
	
	p->validpage_balancing = 0;
	p->max_plane = 0;
	p->target_plane = 0;
	p->substituted = 0;
	p->nop_count = 0;
	p->cumulative_adjust_count = 0;
	p->gc_copy_count = 0;

	return 0;
}

void bdbm_page_ftl_destroy (bdbm_drv_info_t* bdi)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;

	if (!p)
		return;
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
	if (p->bai)
		bdbm_abm_destroy (p->bai);
	bdbm_free (p);
}


void bdbm_page_ftl_balancing_finish_check(bdbm_drv_info_t* bdi)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	uint64_t max_ch, max_way;
	uint64_t min_ch, min_way;

	if (p->validpage_balancing != 0)
	{
		// max is  
		max_ch = p->max_plane / np->nr_chips_per_channel;
		max_way = p->max_plane % np->nr_chips_per_channel; 

		min_ch = p->target_plane / np->nr_chips_per_channel;
		min_way = p->target_plane % np->nr_chips_per_channel; 

		if ((p->valid_page_count[max_ch][max_way] <= p->average_valid_count) ||
			(p->valid_page_count[min_ch][min_way] >= p->average_valid_count) ||
			(p->bai->anr_free_blks[min_ch][min_way] < 2))
		{
			p->validpage_balancing = 0;
			p->substituted = 0;
			
			bdbm_msg("Finish: max: %lld, target: %lld, ad cnt: %lld, cp cnt:%lld", p->max_plane, p->target_plane, p->cumulative_adjust_count, p->gc_copy_count);
		}
	}
}

void bdbm_page_ftl_balancing_start_check(bdbm_drv_info_t* bdi)
{
	// check the valid page distribution and set the data structure.
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);

	uint64_t total_valid_page = 0;
	uint64_t max_valid_page = 0;
	uint64_t upper_threshold;
	uint64_t ch, way, unit, unit_count;

	if (p->validpage_balancing == 0)
	{
		unit_count = np->nr_channels * np->nr_chips_per_channel;
		p->substituted = 0;

		// check the max plane.
		for (unit = 0; unit < unit_count; unit++)
		{
			ch = unit / np->nr_chips_per_channel;
			way = unit % np->nr_chips_per_channel; 

			total_valid_page += p->valid_page_count[ch][way];

			if (p->valid_page_count[ch][way] > max_valid_page)
			{
				max_valid_page = p->valid_page_count[ch][way];

				// update ch/way
				p->max_plane = unit;
			}		
		}

		p->average_valid_count = total_valid_page / unit_count;
		upper_threshold = (float)p->average_valid_count * 1.0010;
		
		if (max_valid_page >= upper_threshold)
		{
			uint64_t target_find = 0;
			uint64_t min_valid = 0xffffffffff;

			for (unit = 0; unit < unit_count; unit++)
			{
				ch = unit / np->nr_chips_per_channel;
				way = unit % np->nr_chips_per_channel; 

				if ((p->valid_page_count[ch][way] < min_valid) && (p->bai->anr_free_blks[ch][way] >= 2))
				{
					min_valid = p->valid_page_count[ch][way]; 
					target_find = 1;

					p->target_plane = unit;
				}
			}

			if (target_find)
			{
				p->validpage_balancing = 1; // from max plane to target_plane.

				bdbm_msg("start: max: %lld, target: %lld, ad cnt: %lld, cp cnt:%lld", p->max_plane, p->target_plane, p->cumulative_adjust_count, p->gc_copy_count);
			}
		}
	}
}

void bdbm_page_ftl_print_llm(bdbm_llm_req_t* llm_reqs, uint64_t size)
{
	uint64_t index;
	for (index = 0; index < size; index++)
	{
		if (llm_reqs[index].req_type != 0)
		{
			bdbm_msg("index: %lld, reqType:%llx, ch:%lld, way:%lld, blk:%lld", index, llm_reqs[index].req_type, llm_reqs[index].phyaddr.channel_no, llm_reqs[index].phyaddr.chip_no, llm_reqs[index].phyaddr.block_no);
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
		for (way = 0; way < np->nr_chips_per_channel; way++)
		{	
			unit = ch * np->nr_chips_per_channel + way;

			bdbm_msg("ch:%lld, way:%lld, %lld,%lld,%lld,%lld",ch, way,
				(p->ac_bab[unit])->block_no,
				(p->gc_src_bab[unit])->block_no,
				(p->gc_dst_bab[0][unit])->block_no, unit);
				//(p->gc_dst_bab[1][ch*np->nr_chips_per_channel + way])->block_no,
				//(p->gc_dst_bab[2][ch*np->nr_chips_per_channel + way])->block_no);
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

void bdbm_page_ftl_print_validpage_count(bdbm_drv_info_t* bdi)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;

#if 0
	bdbm_msg("valid page count");
	for (ch = 0; ch < np->nr_channels;  ch++)
	{
		bdbm_msg("ch:%lld: %lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld",ch,		
			p->valid_page_count[ch][0], p->valid_page_count[ch][1], p->valid_page_count[ch][2],p->valid_page_count[ch][3], p->valid_page_count[ch][4], p->valid_page_count[ch][5], p->valid_page_count[ch][6], p->valid_page_count[ch][7]);
	}
#else

		bdbm_msg("%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld",		
//		bdbm_msg("%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld",		
//		bdbm_msg("%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,",		
			p->valid_page_count[0][0], p->valid_page_count[0][1], p->valid_page_count[0][2],p->valid_page_count[0][3], p->valid_page_count[0][4], p->valid_page_count[0][5], p->valid_page_count[0][6], p->valid_page_count[0][7],
			p->valid_page_count[1][0], p->valid_page_count[1][1], p->valid_page_count[1][2],p->valid_page_count[1][3], p->valid_page_count[1][4], p->valid_page_count[1][5], p->valid_page_count[1][6], p->valid_page_count[1][7],
			p->valid_page_count[2][0], p->valid_page_count[2][1], p->valid_page_count[2][2],p->valid_page_count[2][3], p->valid_page_count[2][4], p->valid_page_count[2][5], p->valid_page_count[2][6], p->valid_page_count[2][7],
			p->valid_page_count[3][0], p->valid_page_count[3][1], p->valid_page_count[3][2],p->valid_page_count[3][3], p->valid_page_count[3][4], p->valid_page_count[3][5], p->valid_page_count[3][6], p->valid_page_count[3][7],
			p->valid_page_count[4][0], p->valid_page_count[4][1], p->valid_page_count[4][2],p->valid_page_count[4][3], p->valid_page_count[4][4], p->valid_page_count[4][5], p->valid_page_count[4][6], p->valid_page_count[4][7],
			p->valid_page_count[5][0], p->valid_page_count[5][1], p->valid_page_count[5][2],p->valid_page_count[5][3], p->valid_page_count[5][4], p->valid_page_count[5][5], p->valid_page_count[5][6], p->valid_page_count[5][7],
			p->valid_page_count[6][0], p->valid_page_count[6][1], p->valid_page_count[6][2],p->valid_page_count[6][3], p->valid_page_count[6][4], p->valid_page_count[6][5], p->valid_page_count[6][6], p->valid_page_count[6][7],
			p->valid_page_count[7][0], p->valid_page_count[7][1], p->valid_page_count[7][2],p->valid_page_count[7][3], p->valid_page_count[7][4], p->valid_page_count[7][5], p->valid_page_count[7][6], p->valid_page_count[7][7]);
#endif

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
		for (way = 0; way < np->nr_chips_per_channel; way++)
		{
			uint64_t nTotalEC = 0;
			unit =  (ch * np->nr_chips_per_channel + way);
			
			for (blk = 0; blk < np->nr_blocks_per_chip; blk++)
			{
				blk_idx = ch* np->nr_blocks_per_channel + way * np->nr_blocks_per_chip + blk;

				pblk = p->bai->blocks + blk_idx;
				nTotalEC += pblk->erase_count;
			}

			erase_count[unit] = nTotalEC;
		}
	}		

//		bdbm_msg("EC, %lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld, %lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld", // 32.
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

	
uint32_t bdbm_page_ftl_get_free_ppa (
	bdbm_drv_info_t* bdi, 
	int64_t lpa,
	bdbm_phyaddr_t* ppa)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	bdbm_abm_block_t* b = NULL;
	uint64_t curr_channel;
	uint64_t curr_chip;

	/* get the channel & chip numbers */
	curr_channel = p->curr_puid % np->nr_channels;
	curr_chip = p->curr_puid / np->nr_channels;

	/* get the physical offset of the active blocks */
	b = p->ac_bab[curr_channel * np->nr_chips_per_channel + curr_chip];
	ppa->channel_no =  b->channel_no;
	ppa->chip_no = b->chip_no;
	ppa->block_no = b->block_no;
	ppa->page_no = p->curr_page_ofs;
	ppa->punit_id = BDBM_GET_PUNIT_ID (bdi, ppa);

	/* check some error cases before returning the physical address */
	bdbm_bug_on (ppa->channel_no != curr_channel);
	bdbm_bug_on (ppa->chip_no != curr_chip);
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

			bdbm_page_ftl_print_validpage_count(bdi);
		//	bdbm_page_ftl_print_totalEC(bdi);
		}
	} else {
		/*bdbm_msg ("curr_puid = %llu", p->curr_puid);*/
		p->curr_puid++;
	}

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
	
	if (p->gc_dst_blk_offs[copy_count][unit] == np->nr_pages_per_block)
	{
		/* get active blocks */
		//if (__bdbm_page_ftl_get_active_blocks (np, p->bai, p->ac_bab) != 0) {
		//	bdbm_error ("__bdbm_page_ftl_get_active_blocks failed");
		//	return 1;
		//}

		if ((p->gc_dst_bab[copy_count][unit] = bdbm_abm_get_free_block_prepare (p->bai, unit/np->nr_chips_per_channel, unit%np->nr_chips_per_channel)))
		{
			bdbm_abm_get_free_block_commit (p->bai, p->gc_dst_bab[copy_count][unit]);
			p->gc_dst_bab[copy_count][unit]->info = 11; //dest.
			//if (unit == 0)
			{
			//	bdbm_msg("D:%lld,%lld,%lld,%lld", p->gc_dst_bab[copy_count][unit]->channel_no, p->gc_dst_bab[copy_count][unit]->chip_no, p->gc_dst_bab[copy_count][unit]->block_no, unit);
			}
		}
		else 
		{
			bdbm_error ("bdbm_abm_get_free_block_prepare failed");
			return 1; 
		}
				
		/* ok; go ahead with 0 offset */ 
		p->gc_dst_blk_offs[copy_count][unit] = 0;
	}

	/* get the physical offset of the active blocks */
	b = p->gc_dst_bab[copy_count][unit];

	ppa->channel_no =  b->channel_no;
	ppa->chip_no = b->chip_no;
	ppa->block_no = b->block_no;
	ppa->page_no = p->gc_dst_blk_offs[copy_count][unit];
	ppa->punit_id = BDBM_GET_PUNIT_ID (bdi, ppa); // unit

	p->gc_dst_blk_offs[copy_count][unit]++;

	return 0;
}

uint32_t bdbm_page_ftl_map_lpa_to_ppa (
	bdbm_drv_info_t* bdi, 
	bdbm_logaddr_t* logaddr,
	bdbm_phyaddr_t* phyaddr)
{
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_page_mapping_entry_t* me = NULL;
	int k;

	/* is it a valid logical address */
	for (k = 0; k < np->nr_subpages_per_page; k++) {
		if (logaddr->lpa[k] == -1) {
			/* the correpsonding subpage must be set to invalid for gc */
			bdbm_msg("gc invalidate");
			bdbm_abm_invalidate_page (
				p->bai, 
				phyaddr->channel_no, 
				phyaddr->chip_no,
				phyaddr->block_no,
				phyaddr->page_no,
				k
			);

			p->valid_page_count[phyaddr->channel_no][phyaddr->chip_no]--;
			continue;
		}

		if (logaddr->lpa[k] >= np->nr_subpages_per_ssd) {
			bdbm_error ("LPA is beyond logical space (%llX)", logaddr->lpa[k]);
			return 1;
		}

		/* get the mapping entry for lpa */
		me = &p->ptr_mapping_table[logaddr->lpa[k]];
		bdbm_bug_on (me == NULL);

		/* update the mapping table */
		if (me->status == PFTL_PAGE_VALID) {

//			bdbm_msg("move: Old:%lld, N:%lld", me->phyaddr.page_no,phyaddr->page_no);
			bdbm_abm_invalidate_page (
				p->bai, 
				me->phyaddr.channel_no, 
				me->phyaddr.chip_no,
				me->phyaddr.block_no,
				me->phyaddr.page_no,
				me->sp_off
			);
			
			p->valid_page_count[me->phyaddr.channel_no][me->phyaddr.chip_no]--;
		}
		me->status = PFTL_PAGE_VALID;
		me->phyaddr.channel_no = phyaddr->channel_no;
		me->phyaddr.chip_no = phyaddr->chip_no;
		me->phyaddr.block_no = phyaddr->block_no;
		me->phyaddr.page_no = phyaddr->page_no;
		me->sp_off = k;

		p->valid_page_count[phyaddr->channel_no][phyaddr->chip_no]++;
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
		phyaddr->chip_no = 0;
		phyaddr->block_no = 0;
		phyaddr->page_no = 0;
		phyaddr->punit_id = 0;
		*sp_off = 0;
		ret = 1;
	} else {
		phyaddr->channel_no = me->phyaddr.channel_no;
		phyaddr->chip_no = me->phyaddr.chip_no;
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
				me->phyaddr.chip_no,
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
	if (nr_free_blks <= p->bai->nr_gc_trigger_threshold)
	{
		return 1;
	}


	/* invoke gc when there is only one dirty block (for debugging) */
	/*
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	if (bdbm_abm_get_nr_dirty_blocks (p->bai) > 1) {
		return 1;
	}
	*/

	return 0;
}

/* VICTIM SELECTION - First Selection:
 * select the first dirty block in a list */
bdbm_abm_block_t* __bdbm_page_ftl_victim_selection (
	bdbm_drv_info_t* bdi,
	uint64_t channel_no,
	uint64_t chip_no)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	bdbm_abm_block_t* a = NULL;
	bdbm_abm_block_t* b = NULL;
	struct list_head* pos = NULL;

	a = p->ac_bab[channel_no*np->nr_chips_per_channel + chip_no];
	bdbm_abm_list_for_each_dirty_block (pos, p->bai, channel_no, chip_no) {
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
	uint64_t chip_no)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	bdbm_abm_block_t* a = NULL;
	bdbm_abm_block_t* b = NULL;
	bdbm_abm_block_t* v = NULL;
	bdbm_abm_block_t* d = NULL;
	struct list_head* pos = NULL;

	uint64_t unit = channel_no * np->nr_chips_per_channel + chip_no;

	a = p->ac_bab[unit];
	d = p->gc_dst_bab[0][unit];

	bdbm_abm_list_for_each_dirty_block (pos, p->bai, channel_no, chip_no) {
		b = bdbm_abm_fetch_dirty_block (pos);
		if (a == b)
			continue;
		if (d == b)
			continue;
		if (b->nr_invalid_subpages == np->nr_subpages_per_block) {
			v = b;
			break;
		}
		if (v == NULL) {
			v = b;
			continue;
		}
		if (b->nr_invalid_subpages > v->nr_invalid_subpages)
			v = b;
	}

	return v;
}

/* TODO: need to improve it for background gc */
#if 0
uint32_t bdbm_page_ftl_do_gc (bdbm_drv_info_t* bdi)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	bdbm_hlm_req_gc_t* hlm_gc = &p->gc_hlm;
	uint64_t nr_gc_blks = 0;
	uint64_t nr_llm_reqs = 0;
	uint64_t nr_punits = 0;
	uint64_t i, j, k;
	bdbm_stopwatch_t sw;

	nr_punits = np->nr_channels * np->nr_chips_per_channel;

	/* choose uvictim blocks for individual parallel units */
	bdbm_memset (p->gc_bab, 0x00, sizeof (bdbm_abm_block_t*) * nr_punits);
	bdbm_stopwatch_start (&sw);
	for (i = 0, nr_gc_blks = 0; i < np->nr_channels; i++) {
		for (j = 0; j < np->nr_chips_per_channel; j++) {
			bdbm_abm_block_t* b; 
			if ((b = __bdbm_page_ftl_victim_selection_greedy (bdi, i, j))) {
				p->gc_bab[nr_gc_blks] = b;
				nr_gc_blks++;
			}
		}
	}
	if (nr_gc_blks < nr_punits) {
		/* TODO: we need to implement a load balancing feature to avoid this */
		/*bdbm_warning ("TODO: this warning will be removed with load-balancing");*/
		return 0;
	}

	/* build hlm_req_gc for reads */
	for (i = 0, nr_llm_reqs = 0; i < nr_gc_blks; i++) {
		bdbm_abm_block_t* b = p->gc_bab[i];
		if (b == NULL)
			break;
		for (j = 0; j < np->nr_pages_per_block; j++) {
			bdbm_llm_req_t* r = &hlm_gc->llm_reqs[nr_llm_reqs];
			int has_valid = 0;
			/* are there any valid subpages in a block */
			hlm_reqs_pool_reset_fmain (&r->fmain);
			hlm_reqs_pool_reset_logaddr (&r->logaddr);
			for (k = 0; k < np->nr_subpages_per_page; k++) {
				if (b->pst[j*np->nr_subpages_per_page+k] != BDBM_ABM_SUBPAGE_INVALID) {
					has_valid = 1;
					r->logaddr.lpa[k] = -1; /* the subpage contains new data */
					r->fmain.kp_stt[k] = KP_STT_DATA;
				} else {
					r->logaddr.lpa[k] = -1;	/* the subpage contains obsolate data */
					r->fmain.kp_stt[k] = KP_STT_HOLE;
				}
			}
			/* if it is, selects it as the gc candidates */
			if (has_valid) {
				r->req_type = REQTYPE_GC_READ;
				r->phyaddr.channel_no = b->channel_no;
				r->phyaddr.chip_no = b->chip_no;
				r->phyaddr.block_no = b->block_no;
				r->phyaddr.page_no = j;
				r->phyaddr.punit_id = BDBM_GET_PUNIT_ID (bdi, (&r->phyaddr));
				r->ptr_hlm_req = (void*)hlm_gc;
				r->ret = 0;
				nr_llm_reqs++;
			}
		}
	}

	/*
	bdbm_msg ("----------------------------------------------");
	bdbm_msg ("gc-victim: %llu pages, %llu blocks, %llu us", 
		nr_llm_reqs, nr_gc_blks, bdbm_stopwatch_get_elapsed_time_us (&sw));
	*/

	/* wait until Q in llm becomes empty 
	 * TODO: it might be possible to further optimize this */
	bdi->ptr_llm_inf->flush (bdi);

	if (nr_llm_reqs == 0) 
		goto erase_blks;

	/* send read reqs to llm */
	hlm_gc->req_type = REQTYPE_GC_READ;
	hlm_gc->nr_llm_reqs = nr_llm_reqs;
	atomic64_set (&hlm_gc->nr_llm_reqs_done, 0);
	bdbm_sema_lock (&hlm_gc->done);
	for (i = 0; i < nr_llm_reqs; i++) {
		if ((bdi->ptr_llm_inf->make_req (bdi, &hlm_gc->llm_reqs[i])) != 0) {
			bdbm_error ("llm_make_req failed");
			bdbm_bug_on (1);
		}
	}
	bdbm_sema_lock (&hlm_gc->done);
	bdbm_sema_unlock (&hlm_gc->done);

	/* build hlm_req_gc for writes */
	for (i = 0; i < nr_llm_reqs; i++) {
		bdbm_llm_req_t* r = &hlm_gc->llm_reqs[i];
		r->req_type = REQTYPE_GC_WRITE;	/* change to write */
		for (k = 0; k < np->nr_subpages_per_page; k++) {
			/* move subpages that contain new data */
			if (r->fmain.kp_stt[k] == KP_STT_DATA) {
				r->logaddr.lpa[k] = ((uint64_t*)r->foob.data)[k];
			} else if (r->fmain.kp_stt[k] == KP_STT_HOLE) {
				((uint64_t*)r->foob.data)[k] = -1;
				r->logaddr.lpa[k] = -1;
			} else {
				bdbm_bug_on (1);
			}
		}
		if (bdbm_page_ftl_get_free_ppa (bdi, &r->phyaddr) != 0) {
			bdbm_error ("bdbm_page_ftl_get_free_ppa failed");
			bdbm_bug_on (1);
		}
		if (bdbm_page_ftl_map_lpa_to_ppa (bdi, &r->logaddr, &r->phyaddr) != 0) {
			bdbm_error ("bdbm_page_ftl_map_lpa_to_ppa failed");
			bdbm_bug_on (1);
		}
	}

	/* send write reqs to llm */
	hlm_gc->req_type = REQTYPE_GC_WRITE;
	hlm_gc->nr_llm_reqs = nr_llm_reqs;
	atomic64_set (&hlm_gc->nr_llm_reqs_done, 0);
	bdbm_sema_lock (&hlm_gc->done);
	for (i = 0; i < nr_llm_reqs; i++) {
		if ((bdi->ptr_llm_inf->make_req (bdi, &hlm_gc->llm_reqs[i])) != 0) {
			bdbm_error ("llm_make_req failed");
			bdbm_bug_on (1);
		}
	}
	bdbm_sema_lock (&hlm_gc->done);
	bdbm_sema_unlock (&hlm_gc->done);

	/* erase blocks */
erase_blks:
	for (i = 0; i < nr_gc_blks; i++) {
		bdbm_abm_block_t* b = p->gc_bab[i];
		bdbm_llm_req_t* r = &hlm_gc->llm_reqs[i];
		r->req_type = REQTYPE_GC_ERASE;
		r->logaddr.lpa[0] = -1ULL; /* lpa is not available now */
		r->phyaddr.channel_no = b->channel_no;
		r->phyaddr.chip_no = b->chip_no;
		r->phyaddr.block_no = b->block_no;
		r->phyaddr.page_no = 0;
		r->phyaddr.punit_id = BDBM_GET_PUNIT_ID (bdi, (&r->phyaddr));
		r->ptr_hlm_req = (void*)hlm_gc;
		r->ret = 0;
	}

	/* send erase reqs to llm */
	hlm_gc->req_type = REQTYPE_GC_ERASE;
	hlm_gc->nr_llm_reqs = p->nr_punits;
	atomic64_set (&hlm_gc->nr_llm_reqs_done, 0);
	bdbm_sema_lock (&hlm_gc->done);
	for (i = 0; i < nr_gc_blks; i++) {
		if ((bdi->ptr_llm_inf->make_req (bdi, &hlm_gc->llm_reqs[i])) != 0) {
			bdbm_error ("llm_make_req failed");
			bdbm_bug_on (1);
		}
	}
	bdbm_sema_lock (&hlm_gc->done);
	bdbm_sema_unlock (&hlm_gc->done);

	/* FIXME: what happens if block erasure fails */
	for (i = 0; i < nr_gc_blks; i++) {
		uint8_t ret = 0;
		bdbm_abm_block_t* b = p->gc_bab[i];
		if (hlm_gc->llm_reqs[i].ret != 0) 
			ret = 1;	/* bad block */
		bdbm_abm_erase_block (p->bai, b->channel_no, b->chip_no, b->block_no, ret);
	}

	return 0;
}
#endif

uint32_t bdbm_page_ftl_do_gc (bdbm_drv_info_t* bdi, int64_t lpa)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	bdbm_hlm_req_gc_t* hlm_gc = &p->gc_hlm; // used for read
	bdbm_hlm_req_gc_t* hlm_gc_w = &p->gc_hlm_w; // used for write.
	uint64_t nr_punits = 0;
	uint64_t ch, way, page, subPage, unit;
	bdbm_llm_req_t* req;
	bdbm_abm_block_t* src_blk;
	static uint32_t state = 0;
	
	nr_punits = np->nr_channels * np->nr_chips_per_channel;

	p->nop_count++;
	if ((p->nop_count % 3000000) == 0)
	{
		bdbm_msg("nop count : %lld, read pending : %lld", p->nop_count, hlm_gc->nr_llm_reqs - atomic64_read(&hlm_gc->nr_llm_reqs_done));
		//bdbm_msg("nop count : %lld, write pending : %lld", p->nop_count, hlm_gc_w->nr_llm_reqs - atomic64_read(&hlm_gc_w->nr_llm_reqs_done));

		//bdbm_page_ftl_print_llm(hlm_gc->llm_reqs, nr_punits*3);
	}

	if ((state == 0) && (hlm_gc_w->nr_llm_reqs >= atomic64_read(&hlm_gc_w->nr_llm_reqs_done)+nr_punits))
	{
		// write should not be pended too much
		return 0;
	}

	if ((state == 1) && (hlm_gc->nr_llm_reqs > atomic64_read(&hlm_gc->nr_llm_reqs_done)))
	{
		// read should be finished
		return 1;
	}

	p->nop_count = 0;
	
	if (state == 0)
	{

#ifndef VALID_BALANCE		
	asdfas
		if (p->validpage_balancing == 0)
		{
			bdbm_page_ftl_balancing_start_check(bdi);
		}
		else
		{
			bdbm_page_ftl_balancing_finish_check(bdi);
			p->substituted = 0;
		}
#endif
	
		for (unit = 0; unit < nr_punits; unit++)
		{
			uint64_t cur_unit = unit;			

			if ((p->validpage_balancing != 0) && (p->target_plane == unit))
			{
				// if src block is not available then uses original src blk
				if ((p->gc_src_bab[p->max_plane] != NULL ) && (p->gc_src_bab[p->max_plane]->nr_invalid_subpages < (np->nr_subpages_per_block - 2)))
				{
					cur_unit = p->max_plane;
					p->substituted = 1;
					p->cumulative_adjust_count++;
				}
			}

			src_blk = p->gc_src_bab[cur_unit]; 

			// 1. check the src valid page...
			if ((src_blk != NULL) && (src_blk->nr_invalid_subpages == np->nr_subpages_per_block)) 
			{
				uint64_t erase_req_idx = nr_punits*2 + cur_unit;

				// src is already invalided.
				req = &hlm_gc->llm_reqs[erase_req_idx];
				req->req_type = REQTYPE_GC_ERASE;
				req->logaddr.lpa[0] = -1ULL; /* lpa is not available now */
				req->phyaddr.channel_no = src_blk->channel_no;
				req->phyaddr.chip_no = src_blk->chip_no;
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

				bdbm_abm_erase_block (p->bai, src_blk->channel_no, src_blk->chip_no, src_blk->block_no, /*is bad*/ 0);

				// adjust blck page offset.
				p->gc_src_blk_offs[cur_unit] = np->nr_pages_per_block;

			}

			// 2. Check the Src Block
			if (p->gc_src_blk_offs[cur_unit] == np->nr_pages_per_block)
			{
				ch = cur_unit / np->nr_chips_per_channel;
				way = cur_unit % np->nr_chips_per_channel; 

				// choose victim blocks - new src block
				if ((src_blk != NULL) && (src_blk->nr_invalid_subpages != 0))
				{
					bdbm_msg("ch :%lld, way : %lld, blk :%lld, i:%d, %d",ch, way, src_blk->block_no, src_blk->nr_invalid_subpages, src_blk->info);
					bdbm_bug_on(1);
				}

				if ((src_blk = __bdbm_page_ftl_victim_selection_greedy (bdi, ch, way))) 
				{
					p->gc_src_bab[cur_unit] = src_blk;
					p->gc_src_blk_offs[cur_unit] = 0;

					if (src_blk->nr_invalid_subpages == np->nr_pages_per_block)
					{
						p->gc_src_blk_offs[cur_unit] = np->nr_pages_per_block;
					}						
			

					//	bdbm_msg ("S:%lld,%lld,%lld,i: %d",src_blk->channel_no, src_blk->chip_no, src_blk->block_no, src_blk->nr_invalid_subpages);
				}				
			}
			
			if (src_blk == NULL)
				break;

			// 3. read one page
			req = &hlm_gc->llm_reqs[unit];

			hlm_reqs_pool_reset_fmain (&req->fmain);
			hlm_reqs_pool_reset_logaddr (&req ->logaddr);
			
			for (page = p->gc_src_blk_offs[cur_unit]; page < np->nr_pages_per_block; page++) 
			{
				int has_valid = 0;
				/* are there any valid subpages in a block */
				for (subPage = 0; subPage < np->nr_subpages_per_page; subPage++) 
				{
					if (src_blk->pst[page*np->nr_subpages_per_page+subPage] != BDBM_ABM_SUBPAGE_INVALID) 
					{
						has_valid = 1;
						req->logaddr.lpa[subPage] = -1; /* the subpage contains new data */
						req->fmain.kp_stt[subPage] = KP_STT_DATA;

						//	bdbm_msg ("valid data  pageoffs :%lld, blk invalid  %lld, d\n", page, src_blk->nr_invalid_subpages);
					} 
					else 
					{
						req->logaddr.lpa[subPage] = -1; /* the subpage contains obsolate data */
						req->fmain.kp_stt[subPage] = KP_STT_HOLE;
						//bdbm_msg ("invalid page :%lld", page);
					}
				}
				
				/* if it is, selects it as the gc candidates */
				if (has_valid) {
					req->req_type = REQTYPE_GC_READ;
					req->phyaddr.channel_no = src_blk->channel_no;
					req->phyaddr.chip_no = src_blk->chip_no;
					req->phyaddr.block_no = src_blk->block_no;
					req->phyaddr.page_no = page;
					req->phyaddr.punit_id = BDBM_GET_PUNIT_ID (bdi, (&req->phyaddr));
					req->ptr_hlm_req = (void*)hlm_gc;
					req->ret = 0;

					p->gc_src_blk_offs[cur_unit] = page+1; // update next check point

					/* send read reqs to llm */
					hlm_gc->req_type = REQTYPE_GC_READ;
					hlm_gc->nr_llm_reqs++;

					if ((bdi->ptr_llm_inf->make_req (bdi, req)) != 0) {
						bdbm_error ("llm_make_req failed");
						bdbm_bug_on (1);
					}

					break;
				}
			}						
		}
		
		state = 1;
	}
	else
	{		
		// copy information from read to write hlm...
		hlm_reqs_pool_copy (hlm_gc_w, hlm_gc, np);

		for (way = 0; way < np->nr_chips_per_channel; way++)
		{
			for (ch = 0; ch < np->nr_channels; ch++)
			{
				uint64_t write_bypass = 1;
				
				unit = ch * np->nr_chips_per_channel + way;
				src_blk = p->gc_src_bab[unit];
 
				if (p->substituted == 1)
				{
					if (unit == p->target_plane)
					{
						src_blk = p->gc_src_bab[p->max_plane];
					}
				}

				// 4. Write page
				/* build hlm_req_gc for writes */
				req = &hlm_gc_w->llm_reqs[unit];
				req->req_type = REQTYPE_GC_WRITE; /* change to write */
				
				for (subPage = 0; subPage < np->nr_subpages_per_page; subPage++) {
					/* move subpages that contain new data */
					if (req->fmain.kp_stt[subPage] == KP_STT_DATA) 
					{
						bdbm_phyaddr_t phyaddr;	
						uint64_t sp_offs;					

						req->logaddr.lpa[subPage] = ((uint64_t*)req->foob.data)[subPage];

						bdbm_page_ftl_get_ppa(bdi, req->logaddr.lpa[subPage],&phyaddr,&sp_offs); // previous map check for overwrite during G.C
						if ( (phyaddr.block_no == src_blk->block_no) && (phyaddr.channel_no == src_blk->channel_no) && (phyaddr.chip_no == src_blk->chip_no))
						{
							write_bypass = 0; // TODO>. VALID check
						} 
						else
						{
							bdbm_msg (" Host Update during G.C");
							bdbm_msg (" lpa : %lld, org ch: %lld,%lld,%lld,%d", req->logaddr.lpa[subPage], src_blk->channel_no, src_blk->chip_no, src_blk->block_no, src_blk->info);
							bdbm_msg ("			    new ch: %lld,%lld,%lld, page: %lld", phyaddr.channel_no, phyaddr.chip_no, phyaddr.block_no, phyaddr.page_no);

							bdbm_page_ftl_print_blocks(bdi);
						}
					} else if (req->fmain.kp_stt[subPage] == KP_STT_HOLE) {
						((uint64_t*)req->foob.data)[subPage] = -1;
						req->logaddr.lpa[subPage] = -1;
					} else {
						bdbm_error (" invalid else");
						bdbm_bug_on (1);
					}
				}
				
				if (write_bypass == 1)
				{
					if (src_blk->nr_invalid_subpages != np->nr_pages_per_block)
					{
						bdbm_msg("%lld,%lld,%lld,%lld,%lld,%lld",ch, way, src_blk->block_no, src_blk->nr_invalid_subpages, src_blk->channel_no, src_blk->chip_no);
						bdbm_bug_on (1);
					}	

					continue; // No thing to write - Src must be clean.
				}

				if (unit == 0)
				{
					//bdbm_msg (" src : page :%lld, invalid : %lld, \n",p->gc_src_blk_offs[unit]-1,p->gc_src_bab[unit]->nr_invalid_subpages);
					//bdbm_msg ("gc_write, blk : %lld, page : %lld, lba : %lld \n", req->phyaddr.block_no, req->phyaddr.page_no, req->logaddr.lpa[0]);
					//bdbm_msg ("gc_write, page: %lld \n", req->phyaddr.page_no);
				}

				req->ptr_hlm_req = (void*)hlm_gc_w;
				if (bdbm_page_ftl_get_free_ppa_gc (bdi, unit, src_blk->copy_count, &req->phyaddr) != 0) {
					bdbm_error ("bdbm_page_ftl_get_free_ppa failed");
					bdbm_bug_on (1);
				}
				
				if (bdbm_page_ftl_map_lpa_to_ppa (bdi, &req->logaddr, &req->phyaddr) != 0) {
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

		p->gc_copy_count += nr_punits; 
		state = 0;
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
		b = p->ac_bab[i*np->nr_chips_per_channel + j];

		/* invalidate remaining pages */
		for (k = 0; k < np->nr_subpages_per_page; k++) {
			bdbm_abm_invalidate_page (
				p->bai, 
				b->channel_no, 
				b->chip_no, 
				b->block_no, 
				p->curr_page_ofs, 
				k);
		}
		bdbm_bug_on (b->channel_no != i);
		bdbm_bug_on (b->chip_no != j);

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
		for (j = 0; j < np->nr_chips_per_channel; j++) {
			bdbm_abm_block_t* b = NULL;
			bdbm_llm_req_t* r = NULL;
			uint64_t punit_id = i*np->nr_chips_per_channel+j;

			if ((b = bdbm_abm_get_block (p->bai, i, j, block_no)) == NULL) {
				bdbm_error ("oops! bdbm_abm_get_block failed");
				bdbm_bug_on (1);
			}
			p->gc_src_bab[punit_id] = b;

			r = &hlm_gc->llm_reqs[punit_id];
			r->req_type = REQTYPE_GC_ERASE;
			r->logaddr.lpa[0] = -1ULL; /* lpa is not available now */
			r->phyaddr.channel_no = b->channel_no;
			r->phyaddr.chip_no = b->chip_no;
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

		bdbm_abm_erase_block (p->bai, b->channel_no, b->chip_no, b->block_no, ret);
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
		for (j = 0; j < np->nr_chips_per_channel; j++) {
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
		me[i].phyaddr.chip_no = PFTL_PAGE_INVALID_ADDR;
		me[i].phyaddr.block_no = PFTL_PAGE_INVALID_ADDR;
		me[i].phyaddr.page_no = PFTL_PAGE_INVALID_ADDR;
		me[i].sp_off = -1;
	}

	/* step2: erase all the blocks */
	bdi->ptr_llm_inf->flush (bdi);
	for (i = 0; i < np->nr_blocks_per_chip; i++) {
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
		me[i].phyaddr.chip_no = PFTL_PAGE_INVALID_ADDR;
		me[i].phyaddr.block_no = PFTL_PAGE_INVALID_ADDR;
		me[i].phyaddr.page_no = PFTL_PAGE_INVALID_ADDR;
	}

	/* step2: erase all the blocks */
	bdi->ptr_llm_inf->flush (bdi);
	for (i = 0; i < np->nr_blocks_per_chip; i++) {
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
