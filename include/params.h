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

#ifndef _BLUEDBM_PARAMS_H
#define _BLUEDBM_PARAMS_H

#define DWHONG	// Test
#define FLOW_CTRL
#define DYNAMIC_DMA		// DMA speed slowdown when multi channel DMA happen
#define MAX_COPY_BACK	(0 + 1) // +1 is used for default or external copyback case
//#define INFINITE_COPYBACK	// 

// GC_Operation Mode
#define GC_OPERATION_MODE		0	// 0 - default, 1 - laze mode, 2 - Early mode, 3 Lazy + Early
#define LAZY_CORRECTION_MODE	(GC_OPERATION_MODE % 2)
#define EARLY_CORRECTION_MODE	(GC_OPERATION_MODE / 2)
#define UTILIZATION_LAZY_MODE	(70)
#define UTILIZATION_EARLY_MODE	(50)

#define GENERATION_FACTOR_WEIGHT	80/100  // Same level of invalid page compared to CP_max Blk
#define LAZY_MODE_THRESHOLD			10/10	// CP_max block's proportion over average
#define EARLY_MODE_THRESHOLD		10/10 // whether 90% of tatal blks are CP0 or not

#define PLANE_NUMBER	(1)
#define GC_FACTOR		(0)

#define GC_BACKGROUND_THRESHOLD		(0+5)*2
#define GC_ONDEMAND_THRESHOLD		(0+4)*2 // + MAX_COPY_BACK)

#define KERNEL_SECTOR_SIZE	512					/* kernel sector size is usually set to 512 bytes */
#define KSECTOR_SIZE		KERNEL_SECTOR_SIZE

/* device-type parameters */
typedef enum {
	DEVICE_TYPE_NOT_SPECIFIED = 0,
	DEVICE_TYPE_RAMDRIVE = 1,
	DEVICE_TYPE_RAMDRIVE_INTR,
	DEVICE_TYPE_RAMDRIVE_TIMING, 
	DEVICE_TYPE_BLUEDBM,
	DEVICE_TYPE_USER_DUMMY,
	DEVICE_TYPE_USER_RAMDRIVE,
	DEVICE_TYPE_END,
} bdbm_device_type_t;

/* default parameters for a device driver */
enum BDBM_MAPPING_POLICY {
	MAPPING_POLICY_NOT_SPECIFIED = 0,
	MAPPING_POLICY_NO_FTL,
	MAPPING_POLICY_BLOCK,
	MAPPING_POLICY_RSD,
	MAPPING_POLICY_PAGE,
	MAPPING_POLICY_DFTL,
};

enum BDBM_GC_POLICY {
	GC_POLICY_NOT_SPECIFIED = 0,
	GC_POLICY_MERGE,
	GC_POLICY_RAMDOM,
	GC_POLICY_GREEDY,
	GC_POLICY_COST_BENEFIT,
};

enum BDBM_WL_POLICY {
	WL_POLICY_NOT_SPECIFIED = 0,
	WL_POLICY_NONE,
	WL_POLICY_DUAL_POOL,
};

enum BDBM_QUEUE_POLICY {
	QUEUE_POLICY_NOT_SPECIFIED = 0,
	QUEUE_POLICY_NO,
	QUEUE_POLICY_SINGLE_FIFO,
	QUEUE_POLICY_MULTI_FIFO,
};

enum BDBM_TRIM {
	TRIM_NOT_SPECIFIED = 0, 
	TRIM_ENABLE = 1, /* 1: enable, 2: disable */
	TRIM_DISABLE = 2,
};

enum BDBM_LLM_TYPE {
	LLM_NOT_SPECIFIED = 0,
	LLM_NO_QUEUE,
	LLM_MULTI_QUEUE,
};

enum BDBM_HLM_TYPE {
	HLM_NOT_SPECIFIED = 0,
	HLM_NO_BUFFER,
	HLM_BUFFER,
	HLM_DFTL,
};

enum BDBM_SNAPSHOT {
	SNAPSHOT_DISABLE = 0,
	SNAPSHOT_ENABLE,
};


/* parameter structures */
typedef struct {
	uint32_t gc_policy;
	uint32_t wl_policy;
	uint32_t kernel_sector_size;
	uint32_t queueing_policy;
	uint32_t trim;
	uint32_t llm_type;
	uint32_t hlm_type;
	uint32_t mapping_type;
	uint32_t snapshot;	/* 0: disable (default), 1: enable */
} bdbm_ftl_params;

typedef struct {
	uint64_t nr_channels;
	uint64_t nr_chips_per_channel;
	uint64_t nr_planes;
	uint64_t nr_blocks_per_chip;
	uint64_t nr_pages_per_block;
	uint64_t page_main_size;
	uint64_t page_oob_size;
	uint32_t device_type;
	uint64_t device_capacity_in_byte;
#ifdef DWHONG
	uint64_t page_lsb_prog_time_us;
	uint64_t page_msb_prog_time_us;
//	uint64_t read_dma_time_us;
//	uint64_t prog_dma_time_us;
//	uint64_t gc_read_dma_time_us;	
	uint64_t read_dma_time_us[9];	
	uint64_t prog_dma_time_us[9];
#else
	uint64_t page_prog_time_us;
#endif
	uint64_t page_read_time_us;
	uint64_t block_erase_time_us;

	uint64_t nr_blocks_per_channel;
	uint64_t nr_blocks_per_ssd;
	uint64_t nr_chips_per_ssd;
	uint64_t nr_pages_per_ssd;
	uint64_t nr_subpages_per_block;
	uint64_t nr_subpages_per_page;
	uint64_t nr_subpages_per_ssd; /* subpage size must be4 KB */
} bdbm_device_params_t;

#endif /* _BLUEDBM_PARAMS_H */
