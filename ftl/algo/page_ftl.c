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
	uint64_t block_info[MAX_COPY_BACK];
	
	uint64_t src_valid_page_count;
	uint64_t src_unit_hand;
	uint64_t src_plane_hand;	
	uint64_t src_unit_idx[PLANE_NUMBER][64];
	uint64_t src_plane_idx[PLANE_NUMBER][64];	
	uint64_t dst_offset;	

	uint64_t external_die_difference_count;	
	uint64_t external_partial_valid_count;	
	uint64_t external_refresh_count;	
	uint64_t internal_copy_count;
	
	uint64_t read_req_count;  
	uint64_t gc_subpages_move_unit;
	
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

	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;

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
		for (j = 0; j < np->nr_chips_per_channel; j++) 
		{
			uint64_t plane;			
			/* restore previous active block */
			if ((*bab) != NULL)
			{
				for (plane = 0; plane < np->nr_planes; plane++)
				{
					bdbm_abm_make_dirty_blk(bai, i, j, (*bab)->block_no + plane);
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

		p->block_info[i] = 0;
	}

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

	p->src_valid_page_count = 0;
	p->src_unit_hand = 0;
	p->src_plane_hand = 0;
	p->dst_offset = 0;
	
	p->external_die_difference_count = 0;
	p->external_partial_valid_count = 0;
	p->external_refresh_count = 0;	
	p->internal_copy_count = 0;
	
	p->read_req_count = 0;
	p->gc_subpages_move_unit = p->nr_punits * np->nr_planes * np->nr_subpages_per_page;

	//p->src_unit_idx[0];
	p->gc_copy_count = 0;
	p->nop_count = 0;

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
			for (way = 0; way < np->nr_chips_per_channel; way++)
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

	bdbm_msg(" Blk_distribution:%lld,%lld,%lld,%lld,%lld,%lld", p->block_info[0],p->block_info[1],p->block_info[2],p->block_info[3],p->block_info[4],p->block_info[5]);

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
			
			//bdbm_page_ftl_print_hostWrite();

		//	bdbm_page_ftl_print_WAF();
		//	bdbm_page_ftl_print_totalEC(bdi);
		//	bdbm_msg("Alloc_HostFreeBlk");
			bdbm_page_ftl_print_copyback_info(bdi);

			if ( p->gc_copy_count != 0)
			{
			
				bdbm_msg("external copy %lld, %lld,  %lld %, internal: %lld, refresh: %lld", p->external_die_difference_count, p->gc_copy_count, p->external_die_difference_count *10000/p->gc_copy_count, p->internal_copy_count, p->external_refresh_count);
			}
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
		uint64_t ch = unit/np->nr_chips_per_channel;
		uint64_t way = unit%np->nr_chips_per_channel;
		
		/* restore previous active block */
		if (p->gc_dst_bab[copy_count][unit] != NULL)
		{
			for (plane = 0; plane < np->nr_planes; plane++)
			{
				bdbm_abm_make_dirty_blk(p->bai, ch, way, p->gc_dst_bab[copy_count][unit]->block_no + plane);
			}
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
					phyaddr->chip_no,
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

			/* get the mapping entry for lpa */
			me = &p->ptr_mapping_table[logaddr->lpa[index]];
			bdbm_bug_on (me == NULL);

			/* update the mapping table */
			if (me->status == PFTL_PAGE_VALID) {
				bdbm_abm_invalidate_page (
					p->bai, 
					me->phyaddr.channel_no, 
					me->phyaddr.chip_no,
					me->phyaddr.block_no,
					me->phyaddr.page_no,
					me->sp_off
				);
			}
			me->status = PFTL_PAGE_VALID;
			me->phyaddr.channel_no = phyaddr->channel_no;
			me->phyaddr.chip_no = phyaddr->chip_no;
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
		if (nr_free_blks < p->bai->nr_gc_trigger_threshold)
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

		bdbm_abm_block_t* block;
		bdbm_abm_block_t* victim = NULL;
		struct list_head* pos = NULL;

		uint64_t unit = channel_no * np->nr_chips_per_channel + chip_no;
		uint64_t plane;
		uint64_t max_invalid_pages = 0;
		uint64_t full_invalid_pages = np->nr_subpages_per_block * np->nr_planes; 

		bdbm_abm_list_for_each_dirty_block (pos, p->bai, channel_no, chip_no) 
		{
			uint64_t invalid_pages = 0;
			block = bdbm_abm_fetch_dirty_block (pos);
			
			for (plane = 0; plane < np->nr_planes; plane++)
			{
				if (plane != 0)
				{
					pos = pos->next; // next plane;
				}
				
				invalid_pages += block[plane].nr_invalid_subpages;
			}
			
			if (invalid_pages == full_invalid_pages) 
			{
				victim = block;
				break;
			}
			
			if (victim == NULL) 
			{
				victim = block;
				max_invalid_pages = invalid_pages;
				continue;
			}
			
			if (invalid_pages > max_invalid_pages)
			{
				victim = block;
				max_invalid_pages = invalid_pages;
			}
		}

		return victim;
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
				uint64_t erase_req_idx = p->nr_punits*10 + unit;

				if (src_blk->nr_invalid_subpages != np->nr_subpages_per_block)
				{
					bdbm_msg("%lld, %lld",unit,src_blk->nr_invalid_subpages);  
					bdbm_bug_on(1);
				}
					
				// src is already invalided.
				req = &hlm_gc->llm_reqs[erase_req_idx];
				req->req_type = REQTYPE_GC_ERASE;
				req->logaddr.lpa[0] = 0x7FFFFFFE - unit; /* lpa is not available now */
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

				for (plane = 0; plane < np->nr_planes; plane++)
				{
					bdbm_abm_erase_block (p->bai, src_blk->channel_no, src_blk->chip_no, src_blk->block_no + plane, /*is bad*/ 0);
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
		bdbm_hlm_req_gc_t* hlm_gc = &p->gc_hlm; // used for readi
		bdbm_hlm_req_gc_t* hlm_gc_w = &p->gc_hlm_w; // used for write.

		uint64_t ch, way, unit;
		bdbm_abm_block_t* src_blk;

		for (unit = 0; unit < p->nr_punits; unit++)
		{
			src_blk = p->gc_src_bab[unit]; 

			// 2. Alloc New Src
			bdbm_bug_on(p->gc_src_blk_offs[0][unit] != np->nr_pages_per_block);
			
			ch = unit / np->nr_chips_per_channel;
			way = unit % np->nr_chips_per_channel; 
			
			if (p->bai->nr_free_blks > p->bai->nr_gc_trigger_threshold)
			{
				// we already have enough free blocks, so don't need to do gc.
				continue;
			}

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
				}
			}
		}

		bdbm_msg("Alloc Src : %lld, %lld, %lld", p->src_valid_page_count, hlm_gc->nr_llm_reqs - atomic64_read(&hlm_gc->nr_llm_reqs_done), hlm_gc_w->nr_llm_reqs - atomic64_read(&hlm_gc_w->nr_llm_reqs_done)); 
	}

uint64_t bdbm_page_ftl_gc_read_page(bdbm_drv_info_t* bdi, uint64_t unit, uint64_t plane, uint64_t average_valid)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	bdbm_hlm_req_gc_t* hlm_gc = &p->gc_hlm; // used for read
	bdbm_llm_req_t* req;

	uint64_t src_unit;
	uint64_t src_plane;
	bdbm_abm_block_t* src_blk;
	uint64_t page; 
	uint64_t idx = 0;
	uint64_t valid_subpage_count;
	uint64_t read_dma = 1; // need to execute DMA from NAND.
	uint64_t refresh = 0;
	uint64_t valid_count = 0;
	uint64_t check_distance = 0;
	uint64_t bypassed_page = 0; 
	
	src_blk = p->gc_src_bab[unit];
	
	valid_subpage_count = np->nr_subpages_per_block - src_blk[plane].nr_invalid_subpages;
	
	if ((valid_subpage_count != 0) && (valid_subpage_count + np->nr_subpages_per_page >= average_valid))
	{
		src_unit = unit;
		src_plane = plane;
		
		if (src_blk[plane].copy_count+1 < MAX_COPY_BACK)
		{
			read_dma = 0; // utilize internal copyback
		}
		else
		{
			refresh = 1;
		}
	}
	else
	{
		// read from other NAND.
		uint64_t plane_idx;
		for (plane_idx = 0; plane_idx < np->nr_planes; plane_idx++)
		{
			for (idx = 0; idx < p->nr_punits; idx++)
			{
				src_unit = (p->src_unit_hand + idx) % p->nr_punits;
				src_plane = (p->src_plane_hand + plane_idx + (p->src_unit_hand + idx)/p->nr_punits) % np->nr_planes;
				
				src_blk = p->gc_src_bab[src_unit];
	
				valid_subpage_count = np->nr_subpages_per_block - src_blk[src_plane].nr_invalid_subpages;
				if ((valid_subpage_count > average_valid) || ((valid_subpage_count == average_valid) && (src_unit < unit)))
				{
					p->src_unit_hand = src_unit + 1;
					p->src_plane_hand = src_plane;
					
					break;	//TODO
				}
			}
			
			if (idx != p->nr_punits)
			{
				break;
			}
		}
	}

	p->src_unit_idx[plane][unit] = 0xFF;
	p->src_plane_idx[plane][unit] = 0xFF;
	
	req = &hlm_gc->llm_reqs[p->read_req_count]; // one request per plane
	p->read_req_count++;
	
	hlm_reqs_pool_reset_fmain (&req->fmain);
	hlm_reqs_pool_reset_logaddr (&req ->logaddr);
	
	if (idx == p->nr_punits)
	{
		return 0;
	}
	
	p->src_unit_idx[plane][unit] = src_unit;
	p->src_plane_idx[plane][unit] = src_plane;
	
	for (page = p->gc_src_blk_offs[src_plane][src_unit]; page < np->nr_pages_per_block; page += 8)
	{
		uint64_t pst_idx = page / 8; // 64bit = 8bit x 8page consideration
		uint8_t* valid_bitmap;
		uint64_t index;
		
		if (src_blk[src_plane].pst[pst_idx] == 0)
		{
			p->gc_src_blk_offs[src_plane][src_unit] = page + 1;
			continue;
		}
		
		valid_bitmap = (uint8_t*)(src_blk[src_plane].pst[pst_idx]);
		
		for (index = 0; index < 8; index++)
		{
			if (valid_bitmap[index] != 0)
			{
				switch (valid_bitmap[index])
				{
					case 15:					// 1111
						valid_count = 4;
						break;
						
					case 14: case 13: case 11: case 7: 		// 1110, 1101, 1011, 0111
						valid_count = 3;
						break;
	
					case 12: case 10: case 9: case 6: case 5: case  3:	// 1100, 1010, 1001, 0110, 0101, 0011
						bypassed_page += 2;
						if ((check_distance >= 2) || (valid_subpage_count < bypassed_page + np->nr_subpages_per_page))
						{
							valid_count = 2;
						}
						break;
					
					case 8: case 4: case 2: case 1:			// 1000, 0100, 0010, 0001
						bypassed_page += 1;
						if ((check_distance >= 2) || (valid_subpage_count == bypassed_page))
						{
							valid_count = 1;
						}
						break;
				}
			}
			
			if (valid_count != 0)
			{
				break;
			}
		}
		
		if (valid_count != 0)
		{
			uint64_t subPage; 
			for (subPage = 0; subPage < np->nr_subpages_per_page; subPage++)
			{
				if (valid_bitmap[index] & (0x01 << subPage))
				{
					req->fmain.kp_stt[subPage] = KP_STT_DATA;
				}
			}
	
			valid_bitmap[index] = 0;
			src_blk[src_plane].nr_invalid_subpages += valid_count;
			break;
		}
		check_distance++;
	}
	
	bdbm_bug_on(valid_count == 0);
	req->req_type = REQTYPE_GC_READ;
	req->dma = 0; // 0 - DMA bypass, 1 - DMA
	if (read_dma == 0)
	{
		p->internal_copy_count += valid_count;
	}
	else
	{
		req->dma = valid_count;
		if (refresh == 0)
		{
			p->external_die_difference_count += valid_count; // Die�� �ұ���.
		}
		else
		{
			p->external_refresh_count += valid_count; // Error propagation prevention
		}
	}
	
	req->phyaddr.channel_no = src_blk[src_plane].channel_no;
	req->phyaddr.chip_no = src_blk[src_plane].chip_no;
	req->phyaddr.block_no = src_blk[src_plane].block_no;
	req->phyaddr.page_no = page;
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

	return valid_count;
}


uint64_t bdbm_page_ftl_gc_read_partial_page(bdbm_drv_info_t* bdi, uint64_t average_valid)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	bdbm_hlm_req_gc_t* hlm_gc = &p->gc_hlm; // used for read
	bdbm_llm_req_t* req;

	uint64_t src_unit;
	uint64_t src_plane;
	bdbm_abm_block_t* src_blk;
	uint64_t page; 
	uint64_t idx = 0;
	uint64_t valid_subpage_count;
	uint64_t valid_count = 0;
	uint64_t check_distance = 0;
	uint64_t bypassed_page = 0; 
	uint64_t plane_idx;
	
	// decide the src unit / plane
	for (plane_idx = 0; plane_idx < np->nr_planes; plane_idx++)
	{
		for (idx = 0; idx < p->nr_punits; idx++)
		{
			src_unit = (p->src_unit_hand + idx) % p->nr_punits;
			src_plane = (p->src_plane_hand + plane_idx + (p->src_unit_hand + idx)/p->nr_punits) % np->nr_planes;
			
			src_blk = p->gc_src_bab[src_unit];

			valid_subpage_count = np->nr_subpages_per_block - src_blk[src_plane].nr_invalid_subpages;
			if ((valid_subpage_count + np->nr_subpages_per_page > average_valid) && (valid_subpage_count != 0))
			{
				p->src_unit_hand = src_unit + 1;
				p->src_plane_hand = src_plane;
				
				break;	//TODO
			}
		}
		
		if (idx != p->nr_punits)
		{
			break;
		}
	}

	if (p->read_req_count < p->nr_punits)
	{
		p->src_unit_idx[0][p->read_req_count] = 0xFF;
		p->src_plane_idx[0][p->read_req_count] = 0xFF;
	}
	
	req = &hlm_gc->llm_reqs[p->read_req_count]; // one request per plane
	p->read_req_count++;
	
	hlm_reqs_pool_reset_fmain (&req->fmain);
	hlm_reqs_pool_reset_logaddr (&req ->logaddr);
	
	if (idx == p->nr_punits)
	{
		// nothing to read // assert.
		return 0;
	}
		
	for (page = p->gc_src_blk_offs[src_plane][src_unit]; page < np->nr_pages_per_block; page += 8)
	{
		uint64_t pst_idx = page / 8; // 64bit = 8bit x 8page consideration
		uint8_t* valid_bitmap;
		uint64_t index;
		
		if (src_blk[src_plane].pst[pst_idx] == 0)
		{
			p->gc_src_blk_offs[src_plane][src_unit] = page + 1;
			continue;
		}
		
		valid_bitmap = (uint8_t*)(src_blk[src_plane].pst[pst_idx]);
		
		for (index = 0; index < 8; index++)
		{
			if (valid_bitmap[index] != 0)
			{
				switch (valid_bitmap[index])
				{
					case 15:					// 1111
						bypassed_page += 4;
						if ((check_distance >= 2) || (valid_subpage_count == bypassed_page))
						{
							valid_count = 1;
						}
						break;
						
					case 14: case 13: case 11: case 7: 		// 1110, 1101, 1011, 0111					
						bypassed_page += 3;
						if ((check_distance >= 2) || (valid_subpage_count < bypassed_page + np->nr_subpages_per_page))
						{
							valid_count = 1;
						}
						break;
	
					case 12: case 10: case 9: case 6: case 5: case 3:	// 1100, 1010, 1001, 0110, 0101, 0011
						valid_count = 1;
						break;
					
					case 8: case 4: case 2: case 1:			// 1000, 0100, 0010, 0001
						valid_count = 1;
						break;
				}
			}
			
			if (valid_count != 0)
			{
				break;
			}
		}
		
		if (valid_count != 0)
		{
			uint64_t subPage; 
			for (subPage = 0; subPage < np->nr_subpages_per_page; subPage++)
			{
				if (valid_bitmap[index] & (0x01 << subPage))
				{
					req->fmain.kp_stt[subPage] = KP_STT_DATA;

					valid_bitmap[index] = valid_bitmap[index] & ~(0x01 << subPage);	// clear single bit

					break;
				}
			}
	
			src_blk[src_plane].nr_invalid_subpages += valid_count;	// read out only 1 page
			p->external_partial_valid_count += 1; // Die�� �ұ���.
			break;
		}
		check_distance++;
	}
	
	bdbm_bug_on(valid_count == 0);
	req->req_type = REQTYPE_GC_READ;
	req->dma = valid_count; // 0 - DMA bypass, 1 - DMA
	req->phyaddr.channel_no = src_blk[src_plane].channel_no;
	req->phyaddr.chip_no = src_blk[src_plane].chip_no;
	req->phyaddr.block_no = src_blk[src_plane].block_no;
	req->phyaddr.page_no = page;
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

	return valid_count;
}

uint32_t bdbm_page_ftl_gc_read_state_adv(bdbm_drv_info_t* bdi)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	uint64_t unit, plane;
	uint64_t average_valid;
	uint64_t remaining_sub_page = p->nr_punits * np->nr_planes * np->nr_subpages_per_page;

	p->read_req_count = 0;

	// 1. Check Src Validity
	if (p->src_valid_page_count == 0)
	{
		bdbm_page_ftl_restore_srcblk(bdi);
		bdbm_page_ftl_alloc_srcblk(bdi);

		if (p->src_valid_page_count == 0)
		{	
			// don't need to do gc. sufficient free block.
			return 0;
		}
	}

	average_valid = ((p->src_valid_page_count + p->nr_punits - 1) / p->nr_punits) / np->nr_planes;

	if (p->src_valid_page_count >= remaining_sub_page)
	{
		// 2. read nr_punits page - as large as possible
		for (plane = 0; plane < np->nr_planes; plane++)
		{
			for (unit = 0; unit < p->nr_punits; unit++)
			{
				uint64_t read_count = bdbm_page_ftl_gc_read_page(bdi, unit, plane, average_valid);
				remaining_sub_page -= read_count;
			}								
		}
	}
	else
	{
		//
	}

	// 3. read remained page.
	while (remaining_sub_page)
	{
 		uint64_t read_count = bdbm_page_ftl_gc_read_partial_page(bdi, average_valid);
		
		if (read_count != 0)
		{
			remaining_sub_page -= read_count;
		}
		else
		{
			bdbm_bug_on(average_valid >= np->nr_subpages_per_page);
			break;
		}
	}

	return 1;
}

uint32_t bdbm_page_ftl_gc_write_state_adv(bdbm_drv_info_t* bdi)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_device_params_t* np = BDBM_GET_DEVICE_PARAMS (bdi);
	bdbm_hlm_req_gc_t* hlm_gc = &p->gc_hlm; // used for read
	bdbm_hlm_req_gc_t* hlm_gc_w = &p->gc_hlm_w; // used for write.
	uint64_t ch, way, subPage, unit;
	bdbm_llm_req_t* req = NULL;
	bdbm_abm_block_t* src_blk = NULL;

	// copy information from read to write hlm...
	hlm_reqs_pool_compaction(hlm_gc_w, hlm_gc, np, p->dst_offset, p->read_req_count);

	for (way = 0; way < np->nr_chips_per_channel; way++)
	{
		for (ch = 0; ch < np->nr_channels; ch++)
		{
			uint64_t src_unit;
			uint64_t write_bypass = 1; 
			uint64_t dst_index = 0; // default			
	
			unit = ch * np->nr_chips_per_channel + way;
			src_unit = p->src_unit_idx[0][unit];
			if (src_unit != 0xFF)
			{
				src_blk = p->gc_src_bab[src_unit];
			}
			
			// using. src->copy_count info, src/dst plane information.
			if (unit == src_unit)
			{
				dst_index = src_blk->copy_count + 1;
				if (dst_index == MAX_COPY_BACK)
				{
					dst_index = 0;
				}
			}

			// Write page
			/* build hlm_req_gc for writes */
			req = &hlm_gc_w->llm_reqs[p->dst_offset+unit];
			req->req_type = REQTYPE_GC_WRITE; /* change to write */

			for (subPage = 0; subPage < np->nr_subpages_per_page; subPage++) 
			{
				/* move subpages that contain new data */
				if (req->fmain.kp_stt[subPage] == KP_STT_DATA) 
				{
					bdbm_phyaddr_t phyaddr; 
					uint64_t sp_offs;					

					req->logaddr.lpa[subPage] = ((uint64_t*)req->foob.data)[subPage];

/*					Current GC operation is not preemtable. So Host update during G.C can't be happend.

					bdbm_page_ftl_get_ppa(bdi, req->logaddr.lpa[subPage],&phyaddr,&sp_offs); // previous map check for overwrite during G.C
					if ( (phyaddr.block_no == src_blk->block_no) && (phyaddr.channel_no == src_blk->channel_no) && (phyaddr.chip_no == src_blk->chip_no))
					{
						write_bypass = 0; // TODO>. VALID check
					} 
					else
					{
						bdbm_msg (" Host Update during G.C");
						bdbm_msg (" lpa : %lld, org ch: %lld,%lld,%lld,%d", req->logaddr.lpa[subPage], src_blk->channel_no, src_blk->chip_no, src_blk->block_no, src_blk->info);
						bdbm_msg (" 			new ch: %lld,%lld,%lld, page: %lld", phyaddr.channel_no, phyaddr.chip_no, phyaddr.block_no, phyaddr.page_no);

						bdbm_page_ftl_print_blocks(bdi);
					}
*/					
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

			if (bdbm_page_ftl_map_lpa_to_ppa (bdi, &req->logaddr, &req->phyaddr) != 0) 
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

	p->gc_copy_count += p->gc_subpages_move_unit;

	if (p->src_valid_page_count > p->gc_subpages_move_unit)
	{
		p->src_valid_page_count -= p->gc_subpages_move_unit; 
	}
	else
	{
		p->src_valid_page_count = 0;
	}

	p->dst_offset = req->phyaddr.page_no * p->nr_punits;
		
//	bdbm_msg("	write page : %lld", p->src_valid_page_count);
 
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

	for (way = 0; way < np->nr_chips_per_channel; way++)
	{
		for (ch = 0; ch < np->nr_channels; ch++)
		{
			uint64_t src_unit;
			uint64_t write_bypass = 1; 
			uint64_t dst_index = 0; // default			
			uint64_t write_dma = 1; // need to execute DMA to NAND.		
	
			unit = ch * np->nr_chips_per_channel + way;
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
					if ( (phyaddr.block_no == src_blk->block_no) && (phyaddr.channel_no == src_blk->channel_no) && (phyaddr.chip_no == src_blk->chip_no))
					{
						write_bypass = 0; // TODO>. VALID check
					} 
					else
					{
						bdbm_msg (" Host Update during G.C");
						bdbm_msg (" lpa : %lld, org ch: %lld,%lld,%lld,%d", req->logaddr.lpa[subPage], src_blk->channel_no, src_blk->chip_no, src_blk->block_no, src_blk->info);
						bdbm_msg (" 			new ch: %lld,%lld,%lld, page: %lld", phyaddr.channel_no, phyaddr.chip_no, phyaddr.block_no, phyaddr.page_no);

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

			if (bdbm_page_ftl_map_lpa_to_ppa (bdi, &req->logaddr, &req->phyaddr) != 0) 
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

uint32_t bdbm_page_ftl_do_gc (bdbm_drv_info_t* bdi, int64_t lpa)
{
	bdbm_page_ftl_private_t* p = _ftl_page_ftl.ptr_private;
	bdbm_hlm_req_gc_t* hlm_gc = &p->gc_hlm; // used for read
	bdbm_hlm_req_gc_t* hlm_gc_w = &p->gc_hlm_w; // used for write.

	static uint32_t state = 0;
	
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

	p->nop_count = 0;
	
	if (state == 0)
	{
		state = bdbm_page_ftl_gc_read_state_adv(bdi);
	}
	else
	{		
		state = bdbm_page_ftl_gc_write_state_adv(bdi);
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
