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
#include <linux/slab.h>
#include <linux/interrupt.h>

#include <linux/workqueue.h> /* workqueue */

#elif defined (USER_MODE)
#include <stdio.h>
#include <stdint.h>

#else
#error Invalid Platform (KERNEL_MODE or USER_MODE)
#endif

#include "debug.h"
#include "umemory.h"
#include "bdbm_drv.h"
#include "ufile.h"
#include "dev_ramssd.h"

//#define DATA_CHECK

#ifdef DUMMY_SSD
#ifdef DATA_CHECK
	#error "data check is not allowed on dummy ssd mode"
#endif
#endif


#if defined (DATA_CHECK)
static void* __ptr_ramssd_data = NULL;
static uint8_t* __get_ramssd_data_addr (dev_ramssd_info_t* ri, uint64_t lpa)
{
	uint64_t ramssd_addr = -1;
	if (ri->np->nr_subpages_per_page == 1)
		ramssd_addr = ri->np->page_main_size * lpa;
	else
		ramssd_addr = KPAGE_SIZE * lpa;
	return ((uint8_t*)__ptr_ramssd_data) + ramssd_addr;
}
static void __display_hex_values (uint8_t* host, uint8_t* back)
{
	bdbm_msg (" * HOST: %x %x %x %x %x != FLASH: %x %x %x %x %x", 
		host[0], host[1], host[2], host[3], host[4], 
		back[0], back[1], back[2], back[3], back[4]);
}
#if 0
static void __display_hex_values_all (uint8_t* host, uint8_t* back)
{
	int i = 0;
	for (i = 0; i < KPAGE_SIZE; i+=4) {
		bdbm_msg (" * HOST: %x %x %x %x != FLASH: %x %x %x %x", 
			host[i+0], host[i+1], host[i+2], host[i+3],
			back[i+0], back[i+1], back[i+2], back[i+3]);
	}
}
#endif
#endif

/* Functions for Managing DRAM SSD */
static uint8_t* __ramssd_page_addr (
	dev_ramssd_info_t* ri,
	uint64_t channel_no,
	uint64_t chip_no,
	uint64_t block_no,
	uint64_t page_no)
{
	uint8_t* ptr_ramssd = NULL;
	uint64_t ramssd_addr = 0;

	/* calculate the address offset */
	ramssd_addr += dev_ramssd_get_channel_size (ri) * channel_no;
	ramssd_addr += dev_ramssd_get_chip_size (ri) * chip_no;
	ramssd_addr += dev_ramssd_get_block_size (ri) * block_no;
	ramssd_addr += dev_ramssd_get_page_size (ri) * page_no;

	/* get the address */
	ptr_ramssd = (uint8_t*)(ri->ptr_ssdram) + ramssd_addr;

	return ptr_ramssd;
}

static uint8_t* __ramssd_block_addr (
	dev_ramssd_info_t* ri,
	uint64_t channel_no,
	uint64_t chip_no,
	uint64_t block_no)
{
	uint8_t* ptr_ramssd = NULL;
	uint64_t ramssd_addr = 0;

	/* calculate the address offset */
	ramssd_addr += dev_ramssd_get_channel_size (ri) * channel_no;
	ramssd_addr += dev_ramssd_get_chip_size (ri) * chip_no;
	ramssd_addr += dev_ramssd_get_block_size (ri) * block_no;

	/* get the address */
	ptr_ramssd = (uint8_t*)(ri->ptr_ssdram) + ramssd_addr;

	return ptr_ramssd;
}

static void* __ramssd_alloc_ssdram (bdbm_device_params_t* ptr_np)
{
	void* ptr_ramssd = NULL;
	uint64_t page_size_in_bytes;
	uint64_t nr_pages_in_ssd;
	uint64_t ssd_size_in_bytes;

	page_size_in_bytes = 
		ptr_np->page_main_size + 
		ptr_np->page_oob_size;

	nr_pages_in_ssd =
		ptr_np->nr_channels *
		ptr_np->nr_chips_per_channel *
		ptr_np->nr_blocks_per_chip *
		ptr_np->nr_pages_per_block;

	ssd_size_in_bytes = 
		nr_pages_in_ssd * 
		page_size_in_bytes;

	bdbm_msg ("=====================================================================");
	bdbm_msg ("RAM DISK INFO");
	bdbm_msg ("=====================================================================");
	bdbm_msg ("the SSD capacity: %llu (B), %llu (KB), %llu (MB)",
		ptr_np->device_capacity_in_byte,
		BDBM_SIZE_KB(ptr_np->device_capacity_in_byte),
		BDBM_SIZE_MB(ptr_np->device_capacity_in_byte));

#ifdef DUMMY_SSD
	page_size_in_bytes = ptr_np->page_oob_size;

	ssd_size_in_bytes = 
		nr_pages_in_ssd * 
		page_size_in_bytes;
#endif

	/* allocate the memory for the SSD */
	if ((ptr_ramssd = (void*)bdbm_malloc
			(ssd_size_in_bytes * sizeof (uint8_t))) == NULL) {
		bdbm_error ("bdbm_malloc failed (size=%llu)", ssd_size_in_bytes * sizeof (uint8_t));
		return NULL;
	}
	bdbm_memset ((uint8_t*)ptr_ramssd, 0xFF, ssd_size_in_bytes * sizeof (uint8_t));

	bdbm_msg ("ramssd addr = %p", ptr_ramssd);
	bdbm_msg ("");

#if defined (DATA_CHECK)
	bdbm_msg ("*** building ptr_ramssd_data begins for data curruption checks...");
	if ((__ptr_ramssd_data = (void*)bdbm_malloc	(ssd_size_in_bytes * sizeof (uint8_t))) == NULL) {
		bdbm_warning ("bdbm_malloc () failed for ptr_ramssd_data");
	}
	bdbm_memset ((uint8_t*)__ptr_ramssd_data, 0xFF, ssd_size_in_bytes * sizeof (uint8_t));
	bdbm_msg ("*** building ptr_ramssd_data done");
#endif

	/* good; return ramssd addr */
	return (void*)ptr_ramssd;
}

static void __ramssd_free_ssdram (void* ptr_ramssd) 
{
#if defined (DATA_CHECK)
	if (__ptr_ramssd_data) {
		bdbm_free (__ptr_ramssd_data);
	}
#endif
	bdbm_free (ptr_ramssd);
}

static uint8_t __ramssd_read_page (
	dev_ramssd_info_t* ri, 
	uint64_t channel_no,
	uint64_t chip_no,
	uint64_t block_no,
	uint64_t page_no,
	kp_stt_t* kp_stt,
	uint8_t** kp_ptr,
	uint8_t* oob_data,
	uint8_t oob,
	uint8_t partial,
	uint64_t type)
{
	uint8_t ret = 0;
	uint8_t* ptr_ramssd_addr = NULL;
#ifndef DUMMY_SSD
	uint32_t nr_kpages;
#endif
	uint32_t loop;

	/* get the memory address for the destined page */
	if ((ptr_ramssd_addr = __ramssd_page_addr (ri, channel_no, chip_no, block_no, page_no)) == NULL) {
		bdbm_error ("invalid ram_addr (%p)", ptr_ramssd_addr);
		ret = 1;
		goto fail;
	}
#ifndef DUMMY_SSD
	/* for better performance, RAMSSD directly copies the SSD data to kernel pages */
	nr_kpages = ri->np->page_main_size / KERNEL_PAGE_SIZE;
	if (ri->np->page_main_size % KERNEL_PAGE_SIZE != 0) {
		bdbm_error ("The page-cache granularity (%lu) is not matched to the flash page size (%llu)", 
			KERNEL_PAGE_SIZE, ri->np->page_main_size);
		ret = 1;
		goto fail;
	}

	/* copy the main page data to a buffer */
	if (ri->np->nr_subpages_per_page == 1) {
		for (loop = 0; loop < nr_kpages; loop++) {
			if (partial == 1 && kp_stt[loop] == KP_STT_DATA) continue;
			bdbm_memcpy (kp_ptr[loop], ptr_ramssd_addr + KPAGE_SIZE * loop, KPAGE_SIZE);
		}
	} else {
		for (loop = 0; loop < nr_kpages; loop++) {
			if (partial == 1 && kp_stt[loop] == KP_STT_DATA) continue;
			if (partial == 0 && kp_stt[loop] != KP_STT_DATA) continue;
			bdbm_memcpy (kp_ptr[loop], ptr_ramssd_addr + KPAGE_SIZE * loop, KPAGE_SIZE);
		}
	}

	/* copy the OOB data to a buffer */
	if (partial == 0 && oob && oob_data != NULL) {
		bdbm_memcpy (oob_data, 
			ptr_ramssd_addr + ri->np->page_main_size,
			ri->np->page_oob_size
		);
	}
#else
	/* copy the OOB data to a buffer */
	if (partial == 0 && oob && oob_data != NULL) {
		bdbm_memcpy (oob_data, 
				ptr_ramssd_addr,
			ri->np->page_oob_size
		);
	}
#endif



#if defined (DATA_CHECK)
	if (ri->np->nr_subpages_per_page == 1) {
		uint8_t* ptr_data_org = NULL;
		for (loop = 0; loop < nr_kpages; loop++) {
 			int64_t lpa = ((uint64_t*)oob_data)[0];
			if (lpa < 0 || lpa == 0xffffffffffffffff) continue;
			if (partial == 1 && kp_stt[loop] == KP_STT_DATA)	continue;
			ptr_data_org = (uint8_t*)__get_ramssd_data_addr (ri, lpa);
			if (memcmp (kp_ptr[loop], ptr_data_org+(loop*KPAGE_SIZE), KPAGE_SIZE) != 0) {
				bdbm_msg ("[DATA CORRUPTION] lpa=%llu offset=%u type: %lld", lpa, loop, type);
				bdbm_msg ("ch: %lld, way: %lld, block =%llu page=%llu", channel_no, chip_no, block_no, page_no);
				__display_hex_values (kp_ptr[loop], ptr_data_org+(loop*KPAGE_SIZE));
				//bdbm_bug_on(1);
			}
		}
	} else {
		uint8_t* ptr_data_org = NULL;
		for (loop = 0; loop < nr_kpages; loop++) {
			int64_t lpa = ((uint64_t*)oob_data)[loop];
			if (lpa < 0 || lpa == 0xffffffffffffffff) continue;
			if (partial == 1 && kp_stt[loop] == KP_STT_DATA) continue;
			if (partial == 0 && kp_stt[loop] != KP_STT_DATA) continue;
			ptr_data_org = (uint8_t*)__get_ramssd_data_addr (ri, lpa);
			if (memcmp (kp_ptr[loop], ptr_data_org, KPAGE_SIZE) != 0) {
				bdbm_msg ("[READ DATA CORRUPTION] lpa=%llu offset=%u", lpa, loop);
				bdbm_msg ("   ch: %lld, way: %lld, block =%llu page=%llu, loop:%llu", channel_no, chip_no, block_no, page_no, loop);
				__display_hex_values (kp_ptr[loop], ptr_data_org);
			}
		}
	}
#endif

fail:
	return ret;
}

static uint8_t __ramssd_prog_page (
	dev_ramssd_info_t* ri, 
	uint64_t channel_no,
	uint64_t chip_no,
	uint64_t block_no,
	uint64_t page_no,
	kp_stt_t* kp_stt,
	uint8_t** kp_ptr,
	uint8_t* oob_data,
	uint8_t oob)
{
	uint8_t ret = 0;
	uint8_t* ptr_ramssd_addr = NULL;
#ifndef DUMMY_SSD
	uint32_t nr_kpages;
#endif
	uint32_t loop;
	/* get the memory address for the destined page */
	if ((ptr_ramssd_addr = __ramssd_page_addr (ri, channel_no, chip_no, block_no, page_no)) == NULL) {
		bdbm_error ("invalid ram addr (%p)", ptr_ramssd_addr);
		ret = 1;
		goto fail;
	}

#ifndef DUMMY_SSD
	/* for better performance, RAMSSD directly copies the SSD data to pages */
	nr_kpages = ri->np->page_main_size / KERNEL_PAGE_SIZE;
	if (ri->np->page_main_size % KERNEL_PAGE_SIZE != 0) {
		bdbm_error ("The page-cache granularity (%lu) is not matched to the flash page size (%llu)", 
			KERNEL_PAGE_SIZE, ri->np->page_main_size);
		ret = 1;
		goto fail;
	}

	/* copy the main page data to a buffer */
	if (ri->np->nr_subpages_per_page == 1) {
		for (loop = 0; loop < nr_kpages; loop++) {
			bdbm_memcpy (ptr_ramssd_addr + KPAGE_SIZE * loop, kp_ptr[loop], KPAGE_SIZE);
		}
	} else {
		for (loop = 0; loop < nr_kpages; loop++) {
			int64_t lpa = ((int64_t*)oob_data)[loop];
			if (lpa < 0 || lpa == 0xffffffffffffffff) continue;
			if (kp_stt[loop] != KP_STT_DATA) continue;
			bdbm_memcpy (ptr_ramssd_addr + KPAGE_SIZE * loop, kp_ptr[loop], KPAGE_SIZE);
		}
	}

	/* copy the OOB data to a buffer */
	if (oob && oob_data != NULL) {
		bdbm_memcpy (
			ptr_ramssd_addr + ri->np->page_main_size,
			oob_data,
			ri->np->page_oob_size
		);
	}
#else
	/* copy the OOB data to a buffer */
	if (oob && oob_data != NULL) {
		bdbm_memcpy (
			ptr_ramssd_addr,
			oob_data,
			ri->np->page_oob_size
		);
	}
#endif


#if defined (DATA_CHECK)
	if (ri->np->nr_subpages_per_page == 1) {
		uint8_t* ptr_data_org = NULL;
		for (loop = 0; loop < nr_kpages; loop++) {
			int64_t lpa = ((int64_t*)oob_data)[0];
			if (lpa < 0 || lpa == 0xffffffffffffffff) continue;
			ptr_data_org = (uint8_t*)__get_ramssd_data_addr (ri, lpa);
			bdbm_memcpy (ptr_data_org+(loop*KPAGE_SIZE), kp_ptr[loop], KPAGE_SIZE);
		}
	} else {
		uint8_t* ptr_data_org = NULL;
		for (loop = 0; loop < nr_kpages; loop++) {
			int64_t lpa = ((int64_t*)oob_data)[loop];
			if (lpa < 0 || lpa == 0xffffffffffffffff) continue;
			if (kp_stt[loop] != KP_STT_DATA) continue;
			ptr_data_org = (uint8_t*)__get_ramssd_data_addr (ri, lpa);
			bdbm_memcpy (ptr_data_org, kp_ptr[loop], KPAGE_SIZE);
		}
	}
#endif

fail:
	return ret;
}

static uint8_t __ramssd_erase_block (
	dev_ramssd_info_t* ri, 
	uint64_t channel_no,
	uint64_t chip_no,
	uint64_t block_no)
{
	uint8_t* ptr_ram_addr = NULL;

	/* get the memory address for the destined block */
	if ((ptr_ram_addr = __ramssd_block_addr 
			(ri, channel_no, chip_no, block_no)) == NULL) {
		bdbm_error ("invalid ssdram addr (%p)", ptr_ram_addr);
		return 1;
	}

	/* erase the block (set all the values to '1') */
	//memset (ptr_ram_addr, 0xFF, dev_ramssd_get_block_size (ri));

	return 0;
}

static uint32_t __ramssd_send_cmd (
	dev_ramssd_info_t* ri, bdbm_llm_req_t* ptr_req)
{
	uint8_t ret = 0;
	uint8_t use_oob = 1;	/* read or program OOB by default; why not??? */
	uint8_t use_partial = 0;
	uint64_t plane;

	if (ri->np->page_oob_size == 0)
		use_oob = 0;

	switch (ptr_req->req_type) {
	case REQTYPE_RMW_READ:
		use_partial = 1;
	case REQTYPE_READ:
	case REQTYPE_META_READ:
	case REQTYPE_GC_READ:
		ret = __ramssd_read_page (
			ri, 
			ptr_req->phyaddr.channel_no, 
			ptr_req->phyaddr.chip_no, 
			ptr_req->phyaddr.block_no, 
			ptr_req->phyaddr.page_no, 
			ptr_req->fmain.kp_stt,
			ptr_req->fmain.kp_ptr,
			ptr_req->foob.data,
			use_oob,
			use_partial,
			ptr_req->req_type);
		break;

	case REQTYPE_RMW_WRITE:
	case REQTYPE_WRITE:
	case REQTYPE_META_WRITE:
	case REQTYPE_GC_WRITE:
		for (plane = 0; plane < ri->np->nr_planes; plane++)
		{
			uint64_t index = plane*ri->np->nr_subpages_per_page;
			
			ret = __ramssd_prog_page (
				ri, 
				ptr_req->phyaddr.channel_no,
				ptr_req->phyaddr.chip_no,
				ptr_req->phyaddr.block_no + plane,
				ptr_req->phyaddr.page_no,
				&ptr_req->fmain.kp_stt[index],
				&ptr_req->fmain.kp_ptr[index],
				&ptr_req->foob.data[plane*ri->np->page_oob_size],
				use_oob);
		}
		break;

	case REQTYPE_GC_ERASE:
		for (plane = 0; plane < ri->np->nr_planes; plane++)
		{
			ret = __ramssd_erase_block (
				ri, 
				ptr_req->phyaddr.channel_no, 
				ptr_req->phyaddr.chip_no, 
				ptr_req->phyaddr.block_no + plane);
		}
		break;

	case REQTYPE_READ_DUMMY:
		/* do nothing for READ_DUMMY */
		ret = 0;
		break;

	case REQTYPE_TRIM:
		/* do nothing for TRIM */
		ret = 0;
		break;

	default:
		bdbm_error ("invalid command :%lld ch:%lld, way:%lld", ptr_req->req_type, ptr_req->phyaddr.channel_no, ptr_req->phyaddr.chip_no);
		ret = 1;
		break;
	}

	ptr_req->ret = ret;

	return ret;
}

void __ramssd_cmd_done (dev_ramssd_info_t* ri)
{
	uint64_t loop, nr_parallel_units;
	uint64_t nr_channels, nr_ways;
	uint64_t channel, way;
	uint32_t check;

	nr_parallel_units = dev_ramssd_get_chips_per_ssd (ri);
	nr_channels = dev_ramssd_get_channles_per_ssd(ri);
	nr_ways = dev_ramssd_get_chips_per_channel(ri);

#if 0
	for (loop = 0; loop < nr_parallel_units; loop++) {
		bdbm_spin_lock (&ri->ramssd_lock);
		if (ri->ptr_punits[loop].ptr_req != NULL) {
			dev_ramssd_punit_t* punit;
			int64_t elapsed_time_in_us;

			punit = &ri->ptr_punits[loop];
			elapsed_time_in_us = bdbm_stopwatch_get_elapsed_time_us (&punit->sw);

			if (elapsed_time_in_us >= punit->target_elapsed_time_us) {
				void* ptr_req = punit->ptr_req;
				punit->ptr_req = NULL;
				bdbm_spin_unlock (&ri->ramssd_lock);

				/* call the interrupt handler */
				ri->intr_handler (ptr_req);
			} else {
				bdbm_spin_unlock (&ri->ramssd_lock);
			}
		} else {
			bdbm_spin_unlock (&ri->ramssd_lock);
		}
	}
#else
	for (channel = 0; channel < nr_channels; channel++)
	{
		check = 0;
		for (way = 0; way < nr_ways; way++)
		{
			loop = channel*nr_ways + way;

			bdbm_spin_lock (&ri->ramssd_lock);
			if (ri->ptr_punits[loop].ptr_req != NULL) {
				dev_ramssd_punit_t* punit;
				int64_t elapsed_time_in_us;

				punit = &ri->ptr_punits[loop];
				elapsed_time_in_us = bdbm_stopwatch_get_elapsed_time_us (&punit->sw);

				if (elapsed_time_in_us >= punit->target_elapsed_time_us) {
					void* ptr_req = punit->ptr_req;
					punit->ptr_req = NULL;
					bdbm_spin_unlock (&ri->ramssd_lock);

					/* call the interrupt handler */
					ri->intr_handler (ptr_req);
				} else {
					bdbm_spin_unlock (&ri->ramssd_lock);
				}

				check = 1;

				if ( elapsed_time_in_us > 20000)
				{
					bdbm_msg("timer elapsed time : %lld, %lld,%lld,", elapsed_time_in_us, channel, way);
					bdbm_msg("Target_elapese.. : %lld", punit->target_elapsed_time_us);
				}
			} else {
				bdbm_spin_unlock (&ri->ramssd_lock);
			}
		}
		
		if ((check!=0) && (ri->is_busy[channel] !=0))
		{
			uint64_t elapsed_time_in_us = bdbm_stopwatch_get_elapsed_time_us(&(ri->ptr_channels[channel].sw));
			if (elapsed_time_in_us >= ri->ptr_channels[channel].target_elapsed_time_us)
			{
				ri->is_busy[channel] = 0;
				//atomic_dec(&ri->busy_channel);
			}
		}
	}
#endif
}


#if defined (KERNEL_MODE)
static void __dev_ramssd_fops_wq_handler (struct work_struct *w)
{
	dev_ramssd_wq_t* work = (dev_ramssd_wq_t*)w;

	__ramssd_cmd_done ((dev_ramssd_info_t*)work->ri);
}

static enum hrtimer_restart __ramssd_timing_hrtimer_cmd_done (struct hrtimer *ptr_hrtimer)
{
	ktime_t ktime;
	dev_ramssd_info_t* ri;
	
	ri = (dev_ramssd_info_t*)container_of (ptr_hrtimer, dev_ramssd_info_t, hrtimer);

	/* run workqueue */
	queue_work (ri->wq, &ri->works.work);

	ktime = ktime_set (0, 5 * 1000);
	hrtimer_start (&ri->hrtimer, ktime, HRTIMER_MODE_REL);

	return HRTIMER_NORESTART;
}
#endif

uint32_t __ramssd_timing_register_schedule (dev_ramssd_info_t* ri)
{
	switch (ri->emul_mode) {
	case DEVICE_TYPE_RAMDRIVE:
	case DEVICE_TYPE_USER_RAMDRIVE:
		__ramssd_cmd_done (ri);
		break;
#if defined (KERNEL_MODE)
	case DEVICE_TYPE_RAMDRIVE_TIMING:
		break;
#endif
	default:
		__ramssd_cmd_done (ri);
		break;
	}

	return 0;
}

uint32_t __ramssd_timing_create (dev_ramssd_info_t* ri) 
{
	uint32_t ret = 0;

	switch (ri->emul_mode) {
	case DEVICE_TYPE_RAMDRIVE:
	case DEVICE_TYPE_USER_RAMDRIVE:
		break;
#if defined (KERNEL_MODE)
	case DEVICE_TYPE_RAMDRIVE_TIMING: 
		{
			/* create a timer */
			ktime_t ktime;
			hrtimer_init (&ri->hrtimer, CLOCK_REALTIME, HRTIMER_MODE_REL);
			ri->hrtimer.function = __ramssd_timing_hrtimer_cmd_done;
			ktime = ktime_set (0, 500 * 1000);
			hrtimer_start (&ri->hrtimer, ktime, HRTIMER_MODE_REL);

			/* create wq */
			ri->wq = create_singlethread_workqueue ("bdbm_ramssd_wq");
			ri->works.ri = (void*)ri;
			INIT_WORK (&ri->works.work, __dev_ramssd_fops_wq_handler);
		}
		break;
#endif
	default:
		bdbm_error ("invalid timing mode: %d", ri->emul_mode);
		ret = 1;
		break;
	}

	return ret;
}

void __ramssd_timing_destory (dev_ramssd_info_t* ri)
{
	switch (ri->emul_mode) {
	case DEVICE_TYPE_RAMDRIVE:
	case DEVICE_TYPE_USER_RAMDRIVE:
		break;
#if defined (KERNEL_MODE)
	case DEVICE_TYPE_RAMDRIVE_TIMING:
		hrtimer_cancel (&ri->hrtimer);
		if (ri->wq) 
			destroy_workqueue (ri->wq);
		break;
#endif
	default:
		break;
	}
}

/* Functions Exposed to External Files */
dev_ramssd_info_t* dev_ramssd_create (
	bdbm_device_params_t* ptr_np, 
	void (*intr_handler)(void*))
{
	uint64_t loop, nr_parallel_units;
	dev_ramssd_info_t* ri = NULL;

	/* create a ramssd info */
	if ((ri = (dev_ramssd_info_t*)
			bdbm_malloc_atomic (sizeof (dev_ramssd_info_t))) == NULL) {
		bdbm_error ("bdbm_malloc_atomic failed");
		goto fail;
	}

	/* seup parameters */
	ri->intr_handler = intr_handler;
	ri->emul_mode = ptr_np->device_type;
	ri->np = ptr_np;

	/* allocate ssdram space */
	if ((ri->ptr_ssdram = 
			__ramssd_alloc_ssdram (ri->np)) == NULL) {
		bdbm_error ("__ramssd_alloc_ssdram failed");
		goto fail_ssdram;
	}

	/* create parallel units */
	nr_parallel_units = dev_ramssd_get_chips_per_ssd (ri);

	if ((ri->ptr_punits = (dev_ramssd_punit_t*)
			bdbm_malloc_atomic (sizeof (dev_ramssd_punit_t) * nr_parallel_units)) == NULL) {
		bdbm_error ("bdbm_malloc_atomic failed");
		goto fail_punits;
	}
	for (loop = 0; loop < nr_parallel_units; loop++) {
		ri->ptr_punits[loop].ptr_req = NULL;
	}

#ifdef DWHONG
	/* create channel busy checker */
	if ((ri->ptr_channels = (dev_ramssd_channel_t*)
			bdbm_malloc_atomic (sizeof (dev_ramssd_channel_t) * ri->np->nr_channels)) == NULL) {
		bdbm_error ("bdbm_malloc_atomic failed");
		goto fail_punits;
	}

	for (loop = 0; loop < ri->np->nr_channels; loop++) {
		bdbm_stopwatch_start(&(ri->ptr_channels[loop].sw));
		ri->ptr_channels[loop].target_elapsed_time_us = 0;

		ri->is_busy[loop] = 0;
	}

	atomic_set(&ri->busy_channel,0);
#endif

	/* create and register a tasklet */
	if (__ramssd_timing_create (ri) != 0) {
		bdbm_error ("__ramssd_timing_create () failed");
		goto fail_timing;
	}

	/* create spin_lock */
	bdbm_spin_lock_init (&ri->ramssd_lock);

	/* done */
	ri->is_init = 1;

	return ri;

fail_timing:
	bdbm_free_atomic (ri->ptr_punits);

fail_punits:
	__ramssd_free_ssdram (ri->ptr_ssdram);

fail_ssdram:
	bdbm_free_atomic (ri);

fail:
	return NULL;
}

void dev_ramssd_destroy (dev_ramssd_info_t* ri)
{
	/* kill tasklet */
	__ramssd_timing_destory (ri);

	/* free ssdram */
	__ramssd_free_ssdram (ri->ptr_ssdram);

	/* release other stuff */
	bdbm_free_atomic (ri->ptr_punits);
	bdbm_free_atomic (ri);
}

uint32_t dev_ramssd_send_cmd (dev_ramssd_info_t* ri, bdbm_llm_req_t* r)
{
	uint32_t ret;

	if ((ret = __ramssd_send_cmd (ri, r)) == 0) {
		int64_t target_elapsed_time_us = 0;
		uint64_t punit_id = r->phyaddr.punit_id;
		int64_t channel_busy_time = 0;
		int64_t elapsed_time_in_us = 0;

		/* get the target elapsed time depending on the type of req */
		if (ri->emul_mode == DEVICE_TYPE_RAMDRIVE_TIMING) {
			dev_ramssd_channel_t* ptr_channels; 
			uint64_t count = 0;
			uint64_t ch;
			uint64_t dma_count = ri->np->nr_subpages_per_page * ri->np->nr_planes;

			switch (r->req_type) {
			case REQTYPE_GC_WRITE:
				dma_count = r->dma;		
			case REQTYPE_WRITE:
			case REQTYPE_RMW_WRITE:
			case REQTYPE_META_WRITE:

#ifndef  DWHONG
				target_elapsed_time_us = ri->np->page_prog_time_us;
#else

#ifdef DYNAMIC_DMA
				// check the channel busy time
				if (ri->is_busy[r->phyaddr.channel_no] == 0)
				{
					ri->is_busy[r->phyaddr.channel_no] = 1;
					//atomic_inc(&ri->busy_channel);
				}
				
				for (ch = 0; ch < 8; ch++)
				{
					if (ri->is_busy[ch] != 0)
					{
						count++;
					}
				}

				/*
				count = atomic_read(&ri->busy_channel);
				if (count > 8)
				{		
					bdbm_msg("counting issue happend... %lu\n", count);
					count = 8;
				}*/
#endif
				channel_busy_time = ri->np->prog_dma_time_us[count] * dma_count;
				
	//			bdbm_msg("Issue Ch %llu, way %llu, page %llu , paralle : %llu time : %llu\n", r->phyaddr.channel_no, r->phyaddr.chip_no, r->phyaddr.page_no, atomic_read(&ri->busy_channel), channel_busy_time);

				if ((r->req_type == REQTYPE_GC_WRITE) && (r->dma == 0))
				{
					channel_busy_time = 0;
				}

				ptr_channels = ri->ptr_channels + r->phyaddr.channel_no;
				elapsed_time_in_us = bdbm_stopwatch_get_elapsed_time_us(&(ptr_channels->sw));
				if (elapsed_time_in_us >= ptr_channels->target_elapsed_time_us)
				{	// channel is idle.
					// start time update.
					bdbm_stopwatch_start (&(ptr_channels->sw));
					ptr_channels->target_elapsed_time_us = channel_busy_time;
				}
				else
				{	// channel is busy - busy time will be accumulated.
					ptr_channels->target_elapsed_time_us += channel_busy_time;
					channel_busy_time = (ptr_channels->target_elapsed_time_us - elapsed_time_in_us); // update channel_busy time
				}

				//channel busy time + NAND Operation.
				target_elapsed_time_us = channel_busy_time;

				#define NR_PAGE_PER_WORDLINE  (2)
				if (r->phyaddr.page_no % NR_PAGE_PER_WORDLINE)
				{	// LSB page
					target_elapsed_time_us += ri->np->page_lsb_prog_time_us;
				}
				else
				{	// MSB.
					target_elapsed_time_us += ri->np->page_msb_prog_time_us;
				}
#endif	// DWHONG			
				break;

			case REQTYPE_GC_READ:
				dma_count = r->dma;	
			case REQTYPE_READ:
			case REQTYPE_RMW_READ:
			case REQTYPE_META_READ:

				target_elapsed_time_us = ri->np->page_read_time_us;
#ifdef	DWHONG
				// check the channel busy time
				ptr_channels = ri->ptr_channels + r->phyaddr.channel_no;
				channel_busy_time = ri->np->read_dma_time_us;

				if (r->req_type == REQTYPE_GC_READ)
				{
					if (r->dma == 0)
					{					
						channel_busy_time = 0;
					}
					else
					{
#ifdef DYNAMIC_DMA
						// check the channel busy time
						if (ri->is_busy[r->phyaddr.channel_no] == 0)
						{
							ri->is_busy[r->phyaddr.channel_no] = 1;
							//atomic_inc(&ri->busy_channel);
						}

						for (ch = 0; ch < 8; ch++)
						{
							if (ri->is_busy[ch] != 0)
							{
								count++;
							}
						}

						/*						
						count = atomic_read(&ri->busy_channel);
						if ( count > 8)
						{		
							bdbm_msg("counting issue happend... %lu\n", count);
							count = 8;
						}*/
#endif
						channel_busy_time = ri->np->gc_read_dma_time_us[count] * dma_count;
					}
				}

				elapsed_time_in_us = bdbm_stopwatch_get_elapsed_time_us (&(ptr_channels->sw));
				if (elapsed_time_in_us >= ptr_channels->target_elapsed_time_us)
				{	// channel is idle.
					// start time update.
					bdbm_stopwatch_start (&(ptr_channels->sw));
					ptr_channels->target_elapsed_time_us = channel_busy_time;
				}
				else
				{	// channel is busy - busy time will be accumulated.
					ptr_channels->target_elapsed_time_us += channel_busy_time;
					channel_busy_time = (ptr_channels->target_elapsed_time_us - elapsed_time_in_us);
				}

				//channel busy time + NAND Operation.
				target_elapsed_time_us = channel_busy_time;
#endif	// DWHONG

				break;
			case REQTYPE_GC_ERASE:
				target_elapsed_time_us = ri->np->block_erase_time_us;
				
				//bdbm_msg("  Erase: %lld, %lld, %lld", r->phyaddr.channel_no, r->phyaddr.chip_no, target_elapsed_time_us);

				break;
			case REQTYPE_READ_DUMMY:
				target_elapsed_time_us = 0;	/* dummy read */
				break;
			default:
				bdbm_error ("invalid REQTYPE (%u)", r->req_type);
				bdbm_bug_on (1);
				break;
			}

			if (target_elapsed_time_us > 0) {
			//	target_elapsed_time_us -= (target_elapsed_time_us / 10);
			}
		} 
		else {
			target_elapsed_time_us = 0;
		}

		/* register reqs */
		bdbm_spin_lock (&ri->ramssd_lock);
		if (ri->ptr_punits[punit_id].ptr_req == NULL) {
			ri->ptr_punits[punit_id].ptr_req = (void*)r;
			bdbm_stopwatch_start (&ri->ptr_punits[punit_id].sw);
			ri->ptr_punits[punit_id].target_elapsed_time_us = target_elapsed_time_us;
		} else {
			bdbm_error ("More than two requests are assigned to the same parallel unit (ptr=%p, punit=%llu)",
				ri->ptr_punits[punit_id].ptr_req, punit_id);
			bdbm_spin_unlock (&ri->ramssd_lock);
			ret = 1;
			goto fail;
		}
		bdbm_spin_unlock (&ri->ramssd_lock);

		/* register reqs for callback */
		__ramssd_timing_register_schedule (ri);

		if ( channel_busy_time > 20000)
		{
			bdbm_msg("Issue Channel Busy : %lld, unitID: %lld", channel_busy_time, punit_id);
			bdbm_msg("Target_elapese.. : %lld, type:%lld, %lld", target_elapsed_time_us, r->req_type, elapsed_time_in_us );
		}
	}

fail:
	return ret;
}

/* for snapshot */
uint32_t dev_ramssd_load (dev_ramssd_info_t* ri, const char* fn)
{
	bdbm_file_t fp = 0;
	uint64_t len = 0;

	bdbm_msg ("dev_ramssd_load - begin");

	if (ri->ptr_ssdram == NULL) {
		bdbm_error ("ptr_ssdram is NULL");
		return 1;
	}
	
	if ((fp = bdbm_fopen (fn, O_RDWR, 0777)) == 0) {
		bdbm_error ("bdbm_fopen failed");
		return 1;
	}

	bdbm_msg ("dev_ramssd_load: DRAM read starts = %llu", len);
	len = dev_ramssd_get_ssd_size (ri);
	len = bdbm_fread (fp, 0, (uint8_t*)ri->ptr_ssdram, len);
	bdbm_msg ("dev_ramssd_load: DRAM read ends = %llu", len);

	bdbm_fclose (fp);

	bdbm_msg ("dev_ramssd_load - done");

	return 0;
}

uint32_t dev_ramssd_store (dev_ramssd_info_t* ri, const char* fn)
{
	bdbm_file_t fp = 0;
	uint64_t pos = 0;
	uint64_t len = 0;

	bdbm_msg ("dev_ramssd_store - begin");

	if (ri->ptr_ssdram == NULL) {
		bdbm_error ("ptr_ssdram is NULL");
		return 1;
	}
	
	if ((fp = bdbm_fopen (fn, O_CREAT | O_WRONLY, 0777)) == 0) {
		bdbm_error ("bdbm_fopen failed");
		return 1;
	}

	len = dev_ramssd_get_ssd_size (ri);
	bdbm_msg ("dev_ramssd_store: DRAM store starts = %llu", len);
	while (pos < len) {
		pos += bdbm_fwrite (fp, pos, (uint8_t*)ri->ptr_ssdram + pos, len - pos);
	}
	bdbm_fsync (fp);
	bdbm_fclose (fp);

	bdbm_msg ("dev_ramssd_store: DRAM store ends = %llu", pos);
	bdbm_msg ("dev_ramssd_store - end");

	return 0;
}

