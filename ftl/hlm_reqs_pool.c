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
#include "hlm_reqs_pool.h"
#include "umemory.h"


#define DEFAULT_POOL_SIZE		(512)
#define DEFAULT_POOL_INC_SIZE	DEFAULT_POOL_SIZE / 5


uint64_t g_anReqCount[2][257];
uint64_t g_anTotal[2];

#define MAX_REQUEST_ARRAY		1024
uint16_t* gp_write_request;
uint64_t  g_nTotal_write;
uint64_t g_anCumulated_write[128] = {0,};

void __hlm_req_pool_update_workload(uint64_t lpn)
{
	uint32_t slot = (g_nTotal_write/32768); // every 128MB.
	uint16_t* pCount = gp_write_request + (slot % MAX_REQUEST_ARRAY)*64;
	uint64_t plane = lpn % 128;

	pCount[plane/2]++;
	g_anCumulated_write[plane]++;
	g_nTotal_write++;
}

void __hlm_req_pool_print_workload(void)
{
	uint64_t index;

	for (index = 0; index < MAX_REQUEST_ARRAY; index++)
	{
		uint16_t* pCount = gp_write_request + (index)*64;

		bdbm_msg("%lld: %d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d",index,
		pCount[0],pCount[1],pCount[2],pCount[3],pCount[4],pCount[5],pCount[6],pCount[7],
		pCount[8],pCount[9],pCount[10],pCount[11],pCount[12],pCount[13],pCount[14],pCount[15],
		pCount[16],pCount[17],pCount[18],pCount[19],pCount[20],pCount[21],pCount[22],pCount[23],
		pCount[24],pCount[25],pCount[26],pCount[27],pCount[28],pCount[29],pCount[30],pCount[31],
		pCount[32],pCount[33],pCount[34],pCount[35],pCount[36],pCount[37],pCount[38],pCount[39],
		pCount[40],pCount[41],pCount[42],pCount[43],pCount[44],pCount[45],pCount[46],pCount[47],
		pCount[48],pCount[49],pCount[50],pCount[51],pCount[52],pCount[53],pCount[54],pCount[55],
		pCount[56],pCount[57],pCount[58],pCount[59],pCount[60],pCount[61],pCount[62],pCount[63]);
/*
		pCount[64],pCount[65],pCount[66],pCount[67],pCount[68],pCount[69],pCount[70],pCount[71],
		pCount[72],pCount[73],pCount[74],pCount[75],pCount[76],pCount[77],pCount[78],pCount[79],
		pCount[80],pCount[81],pCount[82],pCount[83],pCount[84],pCount[85],pCount[86],pCount[87],
		pCount[88],pCount[89],pCount[90],pCount[91],pCount[92],pCount[93],pCount[94],pCount[95],
		pCount[96],pCount[97],pCount[98],pCount[99],pCount[100],pCount[101],pCount[102],pCount[103],
		pCount[104],pCount[105],pCount[106],pCount[107],pCount[108],pCount[109],pCount[110],pCount[111],
		pCount[112],pCount[113],pCount[114],pCount[115],pCount[116],pCount[117],pCount[118],pCount[119],
		pCount[120],pCount[121],pCount[122],pCount[123],pCount[124],pCount[125],pCount[126],pCount[127]);
*/
	}

	bdbm_msg("total write %lld", g_nTotal_write);
	bdbm_msg("%lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld",\
		g_anCumulated_write[0],g_anCumulated_write[1],g_anCumulated_write[2],g_anCumulated_write[3],g_anCumulated_write[4],g_anCumulated_write[5],g_anCumulated_write[6],g_anCumulated_write[7],
		g_anCumulated_write[8],g_anCumulated_write[9],g_anCumulated_write[10],g_anCumulated_write[11],g_anCumulated_write[12],g_anCumulated_write[13],g_anCumulated_write[14],g_anCumulated_write[15],
		g_anCumulated_write[16],g_anCumulated_write[17],g_anCumulated_write[18],g_anCumulated_write[19],g_anCumulated_write[20],g_anCumulated_write[21],g_anCumulated_write[22],g_anCumulated_write[23],
		g_anCumulated_write[24],g_anCumulated_write[25],g_anCumulated_write[26],g_anCumulated_write[27],g_anCumulated_write[28],g_anCumulated_write[29],g_anCumulated_write[30],g_anCumulated_write[31],
		g_anCumulated_write[32],g_anCumulated_write[33],g_anCumulated_write[34],g_anCumulated_write[35],g_anCumulated_write[36],g_anCumulated_write[37],g_anCumulated_write[38],g_anCumulated_write[39],
		g_anCumulated_write[40],g_anCumulated_write[41],g_anCumulated_write[42],g_anCumulated_write[43],g_anCumulated_write[44],g_anCumulated_write[45],g_anCumulated_write[46],g_anCumulated_write[47],
		g_anCumulated_write[48],g_anCumulated_write[49],g_anCumulated_write[50],g_anCumulated_write[51],g_anCumulated_write[52],g_anCumulated_write[53],g_anCumulated_write[54],g_anCumulated_write[55],
		g_anCumulated_write[56],g_anCumulated_write[57],g_anCumulated_write[58], g_anCumulated_write[59],g_anCumulated_write[60],g_anCumulated_write[61],g_anCumulated_write[62],g_anCumulated_write[63]);

	bdbm_msg("%lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld",\
		g_anCumulated_write[64], g_anCumulated_write[65], g_anCumulated_write[66],g_anCumulated_write[67], g_anCumulated_write[68],g_anCumulated_write[69],g_anCumulated_write[70], g_anCumulated_write[71],
		g_anCumulated_write[72], g_anCumulated_write[73], g_anCumulated_write[74],g_anCumulated_write[75],g_anCumulated_write[76],g_anCumulated_write[77],g_anCumulated_write[78],g_anCumulated_write[79],
		g_anCumulated_write[80],g_anCumulated_write[81],g_anCumulated_write[82],g_anCumulated_write[83],g_anCumulated_write[84],g_anCumulated_write[85],g_anCumulated_write[86],g_anCumulated_write[87],
		g_anCumulated_write[88],g_anCumulated_write[89],g_anCumulated_write[90],g_anCumulated_write[91],g_anCumulated_write[92],g_anCumulated_write[93],g_anCumulated_write[94],g_anCumulated_write[95],
		g_anCumulated_write[96],g_anCumulated_write[97],g_anCumulated_write[98],g_anCumulated_write[99],g_anCumulated_write[100],g_anCumulated_write[101],g_anCumulated_write[102],g_anCumulated_write[103],
		g_anCumulated_write[104],g_anCumulated_write[105],g_anCumulated_write[106],g_anCumulated_write[107],g_anCumulated_write[108],g_anCumulated_write[109],g_anCumulated_write[110],g_anCumulated_write[111],
		g_anCumulated_write[112],g_anCumulated_write[113],g_anCumulated_write[114],g_anCumulated_write[115],g_anCumulated_write[116],g_anCumulated_write[117],g_anCumulated_write[118],g_anCumulated_write[119],
		g_anCumulated_write[120],g_anCumulated_write[121],g_anCumulated_write[122],g_anCumulated_write[123],g_anCumulated_write[124],g_anCumulated_write[125],g_anCumulated_write[126],g_anCumulated_write[127]);
}



void __hlm_req_pool_update_count(uint64_t type, uint64_t size)
{
	g_anTotal[type] += size;

	if (size < 63)
	{
		g_anReqCount[type][size]++;
	}
	else
	{
		g_anReqCount[type][63]++;
	}
}

void __hlm_reqs_pool_reset_count(void)
{
	uint64_t type = 2;
	uint64_t size = 256;
	uint64_t i, j;

	for (i = 0; i < type; i++)
	{
		for (j = 0; j <= size; j++)
		{
			g_anReqCount[i][j] = 0;
		}
		
		g_anTotal[i] = 0;
	}
}

void __hlm_reqs_pool_print_count(void)
{
	bdbm_msg("Read Total :%lld, WriteTotal: %lld", g_anTotal[0], g_anTotal[1]);

	bdbm_msg("Read:%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,\
	%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,\
	%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,\
	%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld",

	g_anReqCount[0][0],g_anReqCount[0][1],	g_anReqCount[0][2],	g_anReqCount[0][3],	g_anReqCount[0][4],	g_anReqCount[0][5],	g_anReqCount[0][6],	g_anReqCount[0][7],
	g_anReqCount[0][8],	g_anReqCount[0][9],	g_anReqCount[0][10],g_anReqCount[0][11],g_anReqCount[0][12],g_anReqCount[0][13],g_anReqCount[0][14],g_anReqCount[0][15],
	g_anReqCount[0][16],g_anReqCount[0][17],g_anReqCount[0][18],g_anReqCount[0][19],g_anReqCount[0][20],g_anReqCount[0][21],g_anReqCount[0][22],g_anReqCount[0][23],
	g_anReqCount[0][24],g_anReqCount[0][25],g_anReqCount[0][26],g_anReqCount[0][27],g_anReqCount[0][28],g_anReqCount[0][29],g_anReqCount[0][30],g_anReqCount[0][31],
	g_anReqCount[0][32],g_anReqCount[0][33],g_anReqCount[0][34],g_anReqCount[0][35],g_anReqCount[0][36],g_anReqCount[0][37],g_anReqCount[0][38],g_anReqCount[0][39],
	g_anReqCount[0][40],g_anReqCount[0][41],g_anReqCount[0][42],g_anReqCount[0][43],g_anReqCount[0][44],g_anReqCount[0][45],g_anReqCount[0][46],g_anReqCount[0][47],
	g_anReqCount[0][48],g_anReqCount[0][49],g_anReqCount[0][50],g_anReqCount[0][51],g_anReqCount[0][52],g_anReqCount[0][53],g_anReqCount[0][54],g_anReqCount[0][55],
	g_anReqCount[0][56],g_anReqCount[0][57],g_anReqCount[0][58],g_anReqCount[0][59],g_anReqCount[0][60],g_anReqCount[0][61],g_anReqCount[0][62],g_anReqCount[0][63]);

	bdbm_msg("Write:\
	%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,\
	%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,\
	%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,\
	%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld",
	
	g_anReqCount[1][0],g_anReqCount[1][1],g_anReqCount[1][2],g_anReqCount[1][3],g_anReqCount[1][4],g_anReqCount[1][5],g_anReqCount[1][6],g_anReqCount[1][7],
	g_anReqCount[1][8],g_anReqCount[1][9],g_anReqCount[1][10],g_anReqCount[1][11],g_anReqCount[1][12],g_anReqCount[1][13],g_anReqCount[1][14],g_anReqCount[1][15],
	g_anReqCount[1][16],g_anReqCount[1][17],g_anReqCount[1][18],g_anReqCount[1][19],g_anReqCount[1][20],g_anReqCount[1][21],g_anReqCount[1][22],g_anReqCount[1][23],
	g_anReqCount[1][24],g_anReqCount[1][25],g_anReqCount[1][26],g_anReqCount[1][27],g_anReqCount[1][28],g_anReqCount[1][29],g_anReqCount[1][30],g_anReqCount[1][31],
	g_anReqCount[1][32],g_anReqCount[1][33],g_anReqCount[1][34],g_anReqCount[1][35],g_anReqCount[1][36],g_anReqCount[1][37],g_anReqCount[1][38],g_anReqCount[1][39],
	g_anReqCount[1][40],g_anReqCount[1][41],g_anReqCount[1][42],g_anReqCount[1][43],g_anReqCount[1][44],g_anReqCount[1][45],g_anReqCount[1][46],g_anReqCount[1][47],
	g_anReqCount[1][48],g_anReqCount[1][49],g_anReqCount[1][50],g_anReqCount[1][51],g_anReqCount[1][52],g_anReqCount[1][53],g_anReqCount[1][54],g_anReqCount[1][55],
	g_anReqCount[1][56],g_anReqCount[1][57],g_anReqCount[1][58],g_anReqCount[1][59],g_anReqCount[1][60],g_anReqCount[1][61],g_anReqCount[1][62],g_anReqCount[1][63]);
}






bdbm_hlm_reqs_pool_t* bdbm_hlm_reqs_pool_create (
	int32_t mapping_unit_size, 
	int32_t io_unit_size)
{
	bdbm_hlm_reqs_pool_t* pool = NULL;
	int in_place_rmw = 0;
	int i = 0;

	/* check input arguments */
	if (mapping_unit_size > io_unit_size) {
		bdbm_error ("oops! mapping_unit_size > io_unit_size (%d > %d)", mapping_unit_size, io_unit_size);
		return NULL;
	}
	if (mapping_unit_size < io_unit_size && mapping_unit_size != KPAGE_SIZE) {
		bdbm_error ("oops! mapping_unit_size is not equal to KERNEL_PAGE_SIZE (%d)", mapping_unit_size);
		return NULL;
	}
	if (io_unit_size > KPAGE_SIZE && mapping_unit_size == io_unit_size) {
		in_place_rmw = 1;
	}

	/* create a pool structure */
	if ((pool = bdbm_malloc (sizeof (bdbm_hlm_reqs_pool_t))) == NULL) {
		bdbm_error ("bdbm_malloc () failed");
		return NULL;
	}

	/* initialize variables */
	bdbm_spin_lock_init (&pool->lock);
	INIT_LIST_HEAD (&pool->used_list);
	INIT_LIST_HEAD (&pool->free_list);
	pool->pool_size = DEFAULT_POOL_SIZE;
	pool->map_unit = mapping_unit_size;
	pool->io_unit = io_unit_size;
	pool->in_place_rmw = in_place_rmw;

	/* add hlm_reqs to the free-list */
	for (i = 0; i < DEFAULT_POOL_SIZE; i++) {
		bdbm_hlm_req_t* item = NULL;
		if ((item = (bdbm_hlm_req_t*)bdbm_malloc (sizeof (bdbm_hlm_req_t))) == NULL) {
			bdbm_error ("bdbm_malloc () failed");
			goto fail;
		}
		hlm_reqs_pool_allocate_llm_reqs (item->llm_reqs, BDBM_BLKIO_MAX_VECS, RP_MEM_VIRT);
		bdbm_sema_init (&item->done);
		list_add_tail (&item->list, &pool->free_list);
	}

	g_nTotal_write = 0;
	if ((gp_write_request = bdbm_malloc (sizeof(uint16_t) * 128 * MAX_REQUEST_ARRAY)) == NULL)
	{
		bdbm_error ("bdbm_malloc () failed");
		goto fail;
	}

	return pool;

fail:
	/* oops! it failed */
	if (pool) {
		struct list_head* next = NULL;
		struct list_head* temp = NULL;
		bdbm_hlm_req_t* item = NULL;
		list_for_each_safe (next, temp, &pool->free_list) {
			item = list_entry (next, bdbm_hlm_req_t, list);
			list_del (&item->list);
			hlm_reqs_pool_release_llm_reqs (item->llm_reqs, BDBM_BLKIO_MAX_VECS, RP_MEM_VIRT);
			bdbm_sema_free (&item->done);
			bdbm_free (item);
		}
		bdbm_spin_lock_destory (&pool->lock);
		bdbm_free (pool);
		pool = NULL;
	}
	return NULL;
}

void bdbm_hlm_reqs_pool_destroy (
	bdbm_hlm_reqs_pool_t* pool)
{
	struct list_head* next = NULL;
	struct list_head* temp = NULL;
	bdbm_hlm_req_t* item = NULL;
	int32_t count = 0;

	if (!pool) return;

	/* free & remove items from the used_list */
	list_for_each_safe (next, temp, &pool->used_list) {
		item = list_entry (next, bdbm_hlm_req_t, list);
		list_del (&item->list);
		hlm_reqs_pool_release_llm_reqs (item->llm_reqs, BDBM_BLKIO_MAX_VECS, RP_MEM_VIRT);
		bdbm_sema_free (&item->done);
		bdbm_free (item);
		count++;
	}

	/* free & remove items from the free_list */
	list_for_each_safe (next, temp, &pool->free_list) {
		item = list_entry (next, bdbm_hlm_req_t, list);
		list_del (&item->list);
		hlm_reqs_pool_release_llm_reqs (item->llm_reqs, BDBM_BLKIO_MAX_VECS, RP_MEM_VIRT);
		bdbm_sema_free (&item->done);
		bdbm_free (item);
		count++;
	}

	if (count != pool->pool_size) {
		bdbm_warning ("oops! count != pool->pool_size (%d != %d)",
			count, pool->pool_size);
	}

	/* free other stuff */
	bdbm_spin_lock_destory (&pool->lock);
	bdbm_free (pool);

//	__hlm_reqs_pool_print_count();
//	__hlm_req_pool_print_workload();
}

bdbm_hlm_req_t* bdbm_hlm_reqs_pool_get_item (
	bdbm_hlm_reqs_pool_t* pool)
{
	struct list_head* pos = NULL;
	bdbm_hlm_req_t* item = NULL;

	bdbm_spin_lock (&pool->lock);

again:
	/* see if there are free items in the free_list */
	list_for_each (pos, &pool->free_list) {
		item = list_entry (pos, bdbm_hlm_req_t, list);
		break;
	}

	/* oops! there are no free items in the free-list */
	if (item == NULL) {
		int i = 0;
		/* add more items to the free-list */
		for (i = 0; i < DEFAULT_POOL_INC_SIZE; i++) {
			bdbm_hlm_req_t* item = NULL;
			if ((item = (bdbm_hlm_req_t*)bdbm_malloc (sizeof (bdbm_hlm_req_t))) == NULL) {
				bdbm_error ("bdbm_malloc () failed");
				goto fail;
			}
			hlm_reqs_pool_allocate_llm_reqs (item->llm_reqs, BDBM_BLKIO_MAX_VECS, RP_MEM_VIRT);
			bdbm_sema_init (&item->done);
			list_add_tail (&item->list, &pool->free_list);
		}
		/* increase the size of the pool */
		pool->pool_size += DEFAULT_POOL_INC_SIZE;

		/* try it again */
		goto again;
	}

	if (item == NULL)
		goto fail;

	/* move it to the used_list */
	list_del (&item->list);
	list_add_tail (&item->list, &pool->used_list);

	bdbm_spin_unlock (&pool->lock);
	return item;

fail:

	bdbm_spin_unlock (&pool->lock);
	return NULL;
}

void bdbm_hlm_reqs_pool_free_item (
	bdbm_hlm_reqs_pool_t* pool, 
	bdbm_hlm_req_t* item)
{
	bdbm_sema_unlock (&item->done);

	bdbm_spin_lock (&pool->lock);
	list_del (&item->list);
	list_add_tail (&item->list, &pool->free_list);
	bdbm_spin_unlock (&pool->lock);
}

static int __hlm_reqs_pool_create_trim_req  (
	bdbm_hlm_reqs_pool_t* pool, 
	bdbm_hlm_req_t* hr,
	bdbm_blkio_req_t* br)
{
	int64_t sec_start, sec_end;

	/* trim boundary sectors */
	sec_start = BDBM_ALIGN_UP (br->bi_offset, NR_KSECTORS_IN(pool->map_unit));
	sec_end = BDBM_ALIGN_DOWN (br->bi_offset + br->bi_size, NR_KSECTORS_IN(pool->map_unit));

	/* initialize variables */
	hr->req_type = br->bi_rw;
	bdbm_stopwatch_start (&hr->sw);
	if (sec_start < sec_end) {
		hr->lpa = (sec_start) / NR_KSECTORS_IN(pool->map_unit);
		hr->len = (sec_end - sec_start) / NR_KSECTORS_IN(pool->map_unit);
	} else {
		hr->lpa = (sec_start) / NR_KSECTORS_IN(pool->map_unit);
		hr->len = 0;
	}
	hr->blkio_req = (void*)br;
	hr->ret = 0;

	return 0;
}

void hlm_reqs_pool_allocate_llm_reqs (
	bdbm_llm_req_t* llm_reqs, 
	int32_t nr_llm_reqs,
	bdbm_rp_mem flag)
{
	int i = 0, j = 0;
	bdbm_flash_page_main_t* fm = NULL;
	bdbm_flash_page_oob_t* fo = NULL;

	/* setup main page */
	for (i = 0; i < nr_llm_reqs; i++) {
		fm = &llm_reqs[i].fmain;
		fo = &llm_reqs[i].foob;
		for (j = 0; j < BDBM_MAX_PAGES; j++)
			if (flag == RP_MEM_PHY)
				fm->kp_pad[j] = (uint8_t*)bdbm_malloc_phy (KPAGE_SIZE);
			else 
				fm->kp_pad[j] = (uint8_t*)bdbm_malloc (KPAGE_SIZE);
		if (flag == RP_MEM_PHY)
			fo->data = (uint8_t*)bdbm_malloc_phy (8*BDBM_MAX_PAGES);
		else
			fo->data = (uint8_t*)bdbm_malloc (8*BDBM_MAX_PAGES);
	}
}

void hlm_reqs_pool_release_llm_reqs (
	bdbm_llm_req_t* llm_reqs, 
	int32_t nr_llm_reqs,
	bdbm_rp_mem flag)
{
	int i = 0, j = 0;
	bdbm_flash_page_main_t* fm = NULL;
	bdbm_flash_page_oob_t* fo = NULL;

	/* setup main page */
	for (i = 0; i < nr_llm_reqs; i++) {
		fm = &llm_reqs[i].fmain;
		fo = &llm_reqs[i].foob;
		for (j = 0; j < BDBM_MAX_PAGES; j++)
			if (flag == RP_MEM_PHY)
				bdbm_free_phy (fm->kp_pad[j]);
			else
				bdbm_free (fm->kp_pad[j]);
		if (flag == RP_MEM_PHY)
			bdbm_free_phy (fo->data);
		else
			bdbm_free (fm->kp_pad[j]);
	}
}

void hlm_reqs_pool_reset_fmain (bdbm_flash_page_main_t* fmain, uint32_t nCount)
{
	int i = 0;
	while (i < nCount) {
		fmain->kp_stt[i] = KP_STT_HOLE;
		fmain->kp_ptr[i] = fmain->kp_pad[i];
		i++;
	}
}

void hlm_reqs_pool_reset_logaddr (bdbm_logaddr_t* logaddr, uint32_t nCount)
{
	int i = 0;
	while (i < nCount) {
		logaddr->lpa[i] = -1;
		i++;
	}
	logaddr->ofs = 0;
}

static int __hlm_reqs_pool_create_write_req (
	bdbm_hlm_reqs_pool_t* pool, 
	bdbm_hlm_req_t* hr,
	bdbm_blkio_req_t* br)
{
	int64_t sec_start, sec_end, pg_start, pg_end;
	int64_t i = 0, j = 0, k = 0;
	int64_t hole = 0, bvec_cnt = 0, nr_llm_reqs;
	int64_t max_map_cnt = pool->io_unit / pool->map_unit;
 
	bdbm_flash_page_main_t* ptr_fm = NULL;
	bdbm_llm_req_t* ptr_lr = NULL;
    
	int nr_valid = 0;
    int add = 0;

	/* expand boundary sectors */
	sec_start = BDBM_ALIGN_DOWN (br->bi_offset, NR_KSECTORS_IN(pool->map_unit));
	sec_end = BDBM_ALIGN_UP (br->bi_offset + br->bi_size, NR_KSECTORS_IN(pool->map_unit));
	bdbm_bug_on (sec_start >= sec_end);

	pg_start = BDBM_ALIGN_DOWN (br->bi_offset, NR_KSECTORS_IN(KPAGE_SIZE)) / NR_KSECTORS_IN(KPAGE_SIZE);
	pg_end = BDBM_ALIGN_UP (br->bi_offset + br->bi_size, NR_KSECTORS_IN(KPAGE_SIZE)) / NR_KSECTORS_IN(KPAGE_SIZE);
	bdbm_bug_on (pg_start >= pg_end);

//	__hlm_req_pool_update_count(1, pg_end - pg_start);

	/* build llm_reqs */
	nr_llm_reqs = BDBM_ALIGN_UP ((sec_end - sec_start), NR_KSECTORS_IN(pool->io_unit)) / NR_KSECTORS_IN(pool->io_unit);
	bdbm_bug_on (nr_llm_reqs > BDBM_BLKIO_MAX_VECS);
	
	ptr_lr = &hr->llm_reqs[0];

	for (i = 0; i < nr_llm_reqs; i++) {
		int fm_ofs = 0;

		ptr_fm = &ptr_lr->fmain;
		hlm_reqs_pool_reset_fmain (ptr_fm, BDBM_MAX_PAGES);
		hlm_reqs_pool_reset_logaddr (&ptr_lr->logaddr, BDBM_MAX_PAGES);

		/* build mapping-units */
		for (j = 0, hole = 0; j < max_map_cnt; j++) {
			/* build kernel-pages */
			ptr_lr->logaddr.lpa[j] = sec_start / NR_KSECTORS_IN(pool->map_unit);
			ptr_lr->logaddr.ofs = -1;

			__hlm_req_pool_update_workload(ptr_lr->logaddr.lpa[j]);

			for (k = 0; k < NR_KPAGES_IN(pool->map_unit); k++) {
				uint64_t pg_off = sec_start / NR_KSECTORS_IN(KPAGE_SIZE);

				if (pg_off >= pg_start && pg_off < pg_end) {
					bdbm_bug_on (bvec_cnt >= br->bi_bvec_cnt);
					if (bvec_cnt >= br->bi_bvec_cnt) {
						bdbm_msg ("%lld %lld", bvec_cnt, br->bi_bvec_cnt);
					}
					ptr_fm->kp_stt[fm_ofs] = KP_STT_DATA;
//					ptr_fm->kp_ptr[fm_ofs] = br->bi_bvec_ptr[bvec_cnt++]; /* assign actual data */
					bdbm_memcpy (ptr_fm->kp_ptr[fm_ofs] , br->bi_bvec_ptr[bvec_cnt++], KPAGE_SIZE); // acutal data copy
				} else {
					hole++;
				}

				/* go to the next */
				sec_start += NR_KSECTORS_IN(KPAGE_SIZE);
				fm_ofs++;
			}

			if (sec_start >= sec_end)
				break;
        }

		/* decide the reqtype for llm_req */
		ptr_lr->req_type = REQTYPE_WRITE;

		if (hole == 1 && pool->in_place_rmw && br->bi_rw == REQTYPE_WRITE) {
			/* NOTE: if there are holes and map-unit is equal to io-unit, we
			*           * should perform old-fashioned RMW operations */
			ptr_lr->req_type = REQTYPE_RMW_READ;
		}

		/* go to the next */
		ptr_lr->ptr_hlm_req = (void*)hr;
		ptr_lr++;
	}
	bdbm_bug_on (bvec_cnt != br->bi_bvec_cnt);
/*
	bdbm_llm_req_t* lr = &hr->llm_reqs[0];
	for (i = 0; i < nr_llm_reqs; i++) {
        bdbm_msg("BEFORE[%d](%lld, %lld, %lld, %lld)(%d, %d, %d, %d)",
                i,
                lr->logaddr.lpa[0],
                lr->logaddr.lpa[1],
                lr->logaddr.lpa[2],
                lr->logaddr.lpa[3],
                lr->fmain.kp_stt[0],
                lr->fmain.kp_stt[1],
                lr->fmain.kp_stt[2],
                lr->fmain.kp_stt[3]
                );
        lr++;
    }
*/
    ptr_lr--;
    for (j = 0; j < pool->io_unit / pool->map_unit; j++) {
        if(ptr_lr->fmain.kp_stt[j] == KP_STT_DATA) nr_valid++;
    }
	
    if (nr_valid > 0 && nr_valid < max_map_cnt){
        bdbm_llm_req_t* next = ptr_lr + 1;
        ptr_lr->logaddr.ofs = 0;
        for (k = 1; k < (pool->io_unit / pool->map_unit) - 1; k++) {
            if (ptr_lr->fmain.kp_stt[k] == KP_STT_DATA) {
                hlm_reqs_pool_reset_fmain (&next->fmain, BDBM_MAX_PAGES);
                hlm_reqs_pool_reset_logaddr (&next->logaddr, BDBM_MAX_PAGES);
                next->req_type = REQTYPE_WRITE;
                next->fmain.kp_stt[k] = KP_STT_DATA;
                next->fmain.kp_ptr[k] = ptr_lr->fmain.kp_ptr[k];
                next->logaddr.lpa[k] = ptr_lr->logaddr.lpa[k];
                next->logaddr.ofs = k;
                next->ptr_hlm_req = (void*)hr;

                ptr_lr->fmain.kp_stt[k] = KP_STT_HOLE;
                ptr_lr->logaddr.lpa[k] = -1;
                next++; add++;
            }
        }
    }

    /* intialize hlm_req */
//    hr->req_type = br->bi_rw;
    hr->req_type = REQTYPE_FLUSH_WRITE;

    bdbm_stopwatch_start (&hr->sw);
    hr->nr_llm_reqs = nr_llm_reqs + add;
    atomic64_set (&hr->nr_llm_reqs_done, 0);
    bdbm_sema_lock (&hr->done);
    hr->blkio_req = (void*)br;
    hr->ret = 0;
/*
    lr = &hr->llm_reqs[0];
    for (i = 0; i < nr_llm_reqs + add; i++) {
        bdbm_msg("AFTER[%d](%lld, %lld, %lld, %lld)(%d, %d, %d, %d)",
                i,
                lr->logaddr.lpa[0],
                lr->logaddr.lpa[1],
                lr->logaddr.lpa[2],
                lr->logaddr.lpa[3],
                lr->fmain.kp_stt[0],
                lr->fmain.kp_stt[1],
                lr->fmain.kp_stt[2],
                lr->fmain.kp_stt[3]
                );
        lr++;
    }
    */

	return 0;
}

static int __hlm_reqs_pool_create_read_req (
	bdbm_hlm_reqs_pool_t* pool, 
	bdbm_hlm_req_t* hr,
	bdbm_blkio_req_t* br)
{
	int64_t pg_start, pg_end, i = 0;
	int64_t offset = 0, bvec_cnt = 0, nr_llm_reqs;
	bdbm_llm_req_t* ptr_lr = NULL;

	pg_start = BDBM_ALIGN_DOWN (br->bi_offset, NR_KSECTORS_IN(KPAGE_SIZE)) / NR_KSECTORS_IN(KPAGE_SIZE);
	pg_end = BDBM_ALIGN_UP (br->bi_offset + br->bi_size, NR_KSECTORS_IN(KPAGE_SIZE)) / NR_KSECTORS_IN(KPAGE_SIZE);
	bdbm_bug_on (pg_start >= pg_end);

//	__hlm_req_pool_update_count(0, pg_end - pg_start);

	/* build llm_reqs */
	nr_llm_reqs = pg_end - pg_start;

	ptr_lr = &hr->llm_reqs[0];
	for (i = 0; i < nr_llm_reqs; i++) {
		offset = pg_start % NR_KPAGES_IN(pool->map_unit);

//		if (pool->in_place_rmw == 0) 
//			bdbm_bug_on (offset != 0);

		hlm_reqs_pool_reset_fmain (&ptr_lr->fmain, BDBM_MAX_PAGES);
		ptr_lr->fmain.kp_stt[offset] = KP_STT_DATA;
		ptr_lr->fmain.kp_ptr[offset] = br->bi_bvec_ptr[bvec_cnt++];

		hlm_reqs_pool_reset_logaddr (&ptr_lr->logaddr, 1);
		ptr_lr->req_type = br->bi_rw;
		ptr_lr->logaddr.lpa[0] = pg_start / NR_KPAGES_IN(pool->map_unit);
		if (pool->in_place_rmw == 1) 
			ptr_lr->logaddr.ofs = 0;		/* offset in llm is already decided */
		else
			ptr_lr->logaddr.ofs = offset;	/* it must be adjusted after getting physical locations */
		ptr_lr->ptr_hlm_req = (void*)hr;

		ptr_lr->dma = 1;

		/* go to the next */
		pg_start++;
		ptr_lr++;
	}

	bdbm_bug_on (bvec_cnt != br->bi_bvec_cnt);

	/* intialize hlm_req */
	hr->req_type = br->bi_rw;
	bdbm_stopwatch_start (&hr->sw);
	hr->nr_llm_reqs = nr_llm_reqs;
	atomic64_set (&hr->nr_llm_reqs_done, 0);
	bdbm_sema_lock (&hr->done);
	hr->blkio_req = (void*)br;
	hr->ret = 0;

	return 0;
}

int bdbm_hlm_reqs_pool_build_req (
	bdbm_hlm_reqs_pool_t* pool, 
	bdbm_hlm_req_t* hr,
	bdbm_blkio_req_t* br)
{
	int ret = 1;

	/* create a hlm_req using a bio */
	if (bdbm_is_read(br->bi_rw)) {
		ret = __hlm_reqs_pool_create_read_req (pool, hr, br);
	} else if (bdbm_is_write(br->bi_rw)) {
		ret = __hlm_reqs_pool_create_write_req (pool, hr, br);
	}
	else if (bdbm_is_trim(br->bi_rw)) {
		ret = __hlm_reqs_pool_create_trim_req (pool, hr, br);
	}
 
	/* are there any errors? */
	if (ret != 0) {
		bdbm_error ("oops! invalid request type: (%llx)", br->bi_rw);
		return 1;
	}

	return 0;
}

void hlm_reqs_pool_relocate_kp (bdbm_llm_req_t* lr, uint64_t new_sp_ofs)
{
	if (new_sp_ofs != lr->logaddr.ofs) {
		lr->fmain.kp_stt[new_sp_ofs] = KP_STT_DATA;
		lr->fmain.kp_ptr[new_sp_ofs] = lr->fmain.kp_ptr[lr->logaddr.ofs];
		lr->fmain.kp_stt[lr->logaddr.ofs] = KP_STT_HOLE;
		lr->fmain.kp_ptr[lr->logaddr.ofs] = lr->fmain.kp_pad[lr->logaddr.ofs];
	}
}

void hlm_reqs_pool_copy(
	bdbm_hlm_req_gc_t* dst, 
	bdbm_hlm_req_gc_t* src, 
	bdbm_device_params_t* np,
	uint64_t dst_offset)
{
	uint64_t subPage, unit;
	uint64_t nr_punits = np->nr_units_per_channel * np->nr_channels;

	bdbm_llm_req_t* dst_req;
	bdbm_llm_req_t* src_req;

	for (unit = 0; unit < nr_punits; unit++) 
	{
		hlm_reqs_pool_reset_fmain (&dst->llm_reqs[dst_offset + unit].fmain, BDBM_MAX_PAGES);

		src_req = &src->llm_reqs[unit];
		dst_req = &dst->llm_reqs[dst_offset + unit];

		for (subPage = 0; subPage < np->nr_subpages_per_page; subPage++) 
		{
			if (src_req->fmain.kp_stt[subPage] == KP_STT_DATA) 
			{
				/* if src has data, copy it to dst */
				dst_req->fmain.kp_stt[subPage] = src_req->fmain.kp_stt[subPage];
				dst_req->fmain.kp_ptr[subPage] = src_req->fmain.kp_ptr[subPage];
				dst_req->logaddr.lpa[subPage] = src_req->logaddr.lpa[subPage];
				((int64_t*)dst_req->foob.data)[subPage] = ((int64_t*)src_req->foob.data)[subPage];
			} 
			else 
			{
				/* otherwise, skip it */
				continue;
			}
		}
	}
}

uint64_t hlm_reqs_pool_compaction(
	bdbm_hlm_req_gc_t* dst,
	bdbm_hlm_req_gc_t* src,
	bdbm_device_params_t* np,
	uint64_t dst_offset,
	uint64_t* pHead_idx,
	uint64_t tail_idx,
	uint64_t* pCount)
{
	uint64_t subpage, unit;
	uint64_t nr_punits = np->nr_units_per_ssd / np->nr_groups_per_die;

	uint64_t src_subpage = 0;
	uint64_t plane;
	uint64_t valid_page_count = 0;
		
	bdbm_llm_req_t* dst_req;
	bdbm_llm_req_t* src_req;

	for (unit = 0; unit < nr_punits; unit++)
	{
		dst_req = &dst->llm_reqs[dst_offset + unit];
		dst_req->dma = 0;
		hlm_reqs_pool_reset_fmain(&dst_req->fmain, BDBM_MAX_PAGES);

		for (plane = 0; plane < np->nr_planes; plane++)
		{			
			for (subpage = 0; subpage < np->nr_subpages_per_page; subpage++)
			{
				uint64_t dst_subpage = plane*np->nr_subpages_per_page + subpage;
				uint64_t src_idx = *pHead_idx;
				
				// partial page read data
				while (src_idx != tail_idx)
				{
					uint64_t data_copy = 0;

					if (src_idx == nr_punits*10) // check end index of buffered data
					{
						src_idx = 0; // move to start of buffered data.
					}

					src_req = src->llm_reqs + src_idx;
					
					if (src_req->fmain.kp_stt[src_subpage] == KP_STT_DATA)
					{
						dst_req->fmain.kp_stt[dst_subpage] = src_req->fmain.kp_stt[src_subpage];
						dst_req->fmain.kp_ptr[dst_subpage] = src_req->fmain.kp_ptr[src_subpage];
						dst_req->logaddr.lpa[dst_subpage]  = src_req->logaddr.lpa[src_subpage];	
						((int64_t*)dst_req->foob.data)[dst_subpage] = ((int64_t*)src_req->foob.data)[src_subpage];

						src_req->fmain.kp_stt[src_subpage] = KP_STT_HOLE;
						(*pCount)--;

						data_copy = 1;
						dst_req->dma++;

						valid_page_count++;

						if ( ((int64_t*)src_req->foob.data)[src_subpage] > 0x1FFFFFF)
						{
							bdbm_msg(" 2. lpn : %llx, index : %lld, subpage: %lld", ((int64_t*)src_req->foob.data)[src_subpage], src_idx, src_subpage);
						}
					}

					src_subpage++;
					if (src_subpage == np->nr_subpages_per_page)
					{
						src_subpage = 0;
						src_idx++;
					}

					if (data_copy != 0)
					{
						*pHead_idx = src_idx;
						break;
					}
				}					
			}			
			// single plane done.
		}
		// copy dst_req is done.
	}

	return valid_page_count;
}


void hlm_reqs_pool_write_compaction (
	bdbm_hlm_req_gc_t* dst, 
	bdbm_hlm_req_gc_t* src, 
	bdbm_device_params_t* np)
{
	uint64_t dst_loop = 0, dst_kp = 0, src_kp = 0, i = 0;
	uint64_t nr_punits = np->nr_units_per_channel * np->nr_channels;

	bdbm_llm_req_t* dst_r = NULL;
	bdbm_llm_req_t* src_r = NULL;

	dst->nr_llm_reqs = 0;
	for (i = 0; i < nr_punits * np->nr_pages_per_block; i++) {
		hlm_reqs_pool_reset_fmain (&dst->llm_reqs[i].fmain, BDBM_MAX_PAGES);
	}

	dst_r = &dst->llm_reqs[0];
	dst->nr_llm_reqs = 1;
	for (i = 0; i < nr_punits * np->nr_pages_per_block; i++) {
		src_r = &src->llm_reqs[i];

		for (src_kp = 0; src_kp < np->nr_subpages_per_page; src_kp++) {
			if (src_r->fmain.kp_stt[src_kp] == KP_STT_DATA) {
				/* if src has data, copy it to dst */
				dst_r->fmain.kp_stt[dst_kp] = src_r->fmain.kp_stt[src_kp];
				dst_r->fmain.kp_ptr[dst_kp] = src_r->fmain.kp_ptr[src_kp];
				dst_r->logaddr.lpa[dst_kp] = src_r->logaddr.lpa[src_kp];
				((int64_t*)dst_r->foob.data)[dst_kp] = ((int64_t*)src_r->foob.data)[src_kp];
			} else {
				/* otherwise, skip it */
				continue;
			}

			/* goto the next llm if all kps are full */
			dst_kp++;
			if (dst_kp == np->nr_subpages_per_page) {
				dst_kp = 0;
				dst_loop++;
				dst_r++;
				dst->nr_llm_reqs++;
			}
		}
	}
}

