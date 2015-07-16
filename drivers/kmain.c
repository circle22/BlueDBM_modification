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

#include <linux/init.h>
#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/version.h>
#include <linux/slab.h>

#include "bdbm_drv.h"
#include "platform.h"
#include "params.h"
#include "debug.h"
#include "host_block.h"
#include "dm_ramdrive.h"
#include "dm_bluesim.h"
#include "dm_bdbme.h"
#include "dm_bluedbm.h"
#include "llm_noq.h"
#include "llm_mq.h"
#include "hlm_nobuf.h"
#include "hlm_buf.h"
#include "hlm_rsd.h"

#include "ftl/no_ftl.h"
#include "ftl/block_ftl.h"
#include "ftl/page_ftl.h"
#include "utils/file.h"

/* for test */
#ifdef BOARD_BSIM
#include "test/nandsim.h"
#endif


/* main data structure */
struct bdbm_drv_info* _bdi = NULL;

struct bdbm_dm_inf_t* setup_risa_device (struct bdbm_drv_info* bdi);


static int init_func_pointers (struct bdbm_drv_info* bdi)
{
	struct bdbm_params* p = bdi->ptr_bdbm_params;

	/* set functions for device manager (dm) */
	switch (p->nand.device_type) {
	case DEVICE_TYPE_RAMDRIVE:
		bdi->ptr_dm_inf = &_dm_ramdrive_inf;
		break;
	case DEVICE_TYPE_BLUESIM:
#ifdef BOARD_BSIM
		bdi->ptr_dm_inf = &_dm_bluesim_inf;
#else
		bdbm_bug_on (1);
#endif
		break;
	case DEVICE_TYPE_BLUEDBM_EMUL:
#ifdef BOARD_BLUEDBM
		bdi->ptr_dm_inf = &_dm_bdbme_inf;
#else
		bdbm_bug_on (1);
#endif
		break;
	case DEVICE_TYPE_BLUEDBM:
#ifdef BOARD_VC707
		bdi->ptr_dm_inf = &_dm_bluedbm_inf;
#else
		bdbm_bug_on (1);
#endif
		break;
	default:
		bdbm_error ("invalid NAND device");
		bdbm_bug_on (1);
		break;
	}

	bdbm_msg ("call setup_risa_device begins");
	bdi->ptr_dm_inf = setup_risa_device (bdi);
	bdbm_msg ("call setup_risa_device ends");

	/* set functions for host */
	switch (p->driver.host_type) {
	case HOST_BLOCK:
		bdi->ptr_host_inf = &_host_block_inf;
		break;
	case HOST_DIRECT:
	default:
		bdbm_error ("invalid host type");
		bdbm_bug_on (1);
		break;
	}

	/* set functions for hlm */
	switch (p->driver.hlm_type) {
	case HLM_NO_BUFFER:
		bdi->ptr_hlm_inf = &_hlm_nobuf_inf;
		break;
	case HLM_BUFFER:
		bdi->ptr_hlm_inf = &_hlm_buf_inf;
		break;
	case HLM_RSD:
		bdi->ptr_hlm_inf = &_hlm_rsd_inf;
		break;
	default:
		bdbm_error ("invalid hlm type");
		bdbm_bug_on (1);
		break;
	}

	/* set functions for llm */
	switch (p->driver.llm_type) {
	case LLM_NO_QUEUE:
		bdi->ptr_llm_inf = &_llm_noq_inf;
		break;
	case LLM_MULTI_QUEUE:
		bdi->ptr_llm_inf = &_llm_mq_inf;
		break;
	default:
		bdbm_error ("invalid llm type");
		bdbm_bug_on (1);
		break;
	}

	/* set functions for ftl */
	switch (p->driver.mapping_type) {
	case MAPPING_POLICY_NO_FTL:
		bdi->ptr_ftl_inf = &_ftl_no_ftl;
		break;
	case MAPPING_POLICY_SEGMENT:
		bdi->ptr_ftl_inf = &_ftl_block_ftl;
		break;
	case MAPPING_POLICY_PAGE:
		bdi->ptr_ftl_inf = &_ftl_page_ftl;
		break;
	default:
		bdbm_error ("invalid ftl type");
		bdbm_bug_on (1);
		break;
	}

	return 0;
}

/*#define RAM_CONSUMER*/

#ifdef RAM_CONSUMER
uint8_t* ptr_ram_consumer = NULL;
#endif

static int __init bdbm_drv_init (void)
{
	struct bdbm_drv_info* bdi = NULL;
	struct bdbm_host_inf_t* host = NULL; 
	struct bdbm_dm_inf_t* dm = NULL;
	struct bdbm_hlm_inf_t* hlm = NULL;
	struct bdbm_llm_inf_t* llm = NULL;
	struct bdbm_ftl_inf_t* ftl = NULL;
#ifdef SNAPSHOT_ENABLE
	uint32_t load = 0;
#endif

#ifdef RAM_CONSUMER
	if ((ptr_ram_consumer = (void*)bdbm_malloc (45000000000 * sizeof (uint8_t))) == NULL) {
		bdbm_error ("bdbm_malloc failed");
		return -ENXIO;
	} else {
		bdbm_error ("RAM_CONSUMER is allocated");
	}
#endif

	/* allocate the memory for bdbm_drv_info */
	if ((bdi = (struct bdbm_drv_info*)bdbm_malloc_atomic (sizeof (struct bdbm_drv_info))) == NULL) {
		bdbm_error ("bdbm_malloc_atomic failed");
		goto fail;
	}
	_bdi = bdi;

	/* get default paramters */
	if ((bdi->ptr_bdbm_params = read_default_params ()) == NULL) {
		bdbm_error ("failed to read the default parameters");
		goto fail;
	}

	/* set function pointers */
	if (init_func_pointers (bdi) != 0) {
		bdbm_error ("failed to initialize function pointers");
		goto fail;
	}

	/* init performance monitor */
	pmu_create (bdi);

	/* open a flash device */
	dm = bdi->ptr_dm_inf;
	if (dm->probe (bdi) != 0) {
		bdbm_error ("failed to probe a flash device");
		goto fail;
	}
	if (dm->open (bdi) != 0) {
		bdbm_error ("failed to open a flash device");
		goto fail;
	}
#ifdef SNAPSHOT_ENABLE
	if (dm->load != NULL) {
		if (dm->load (bdi, "/usr/share/bdbm_drv/dm.dat") != 0) {
			bdbm_msg ("loading 'dm.dat' failed");
			load = 0;
		} else 
			load = 1;
	}
#endif

	/* create a low-level memory manager */
	llm = bdi->ptr_llm_inf;
	if (llm->create (bdi) != 0) {
		bdbm_error ("failed to create llm");
		goto fail;
	}

	/* create a logical-to-physical mapping manager */
	ftl = bdi->ptr_ftl_inf;
	if (ftl->create (bdi) != 0) {
		bdbm_error ("failed to create ftl");
		goto fail;
	}
#ifdef SNAPSHOT_ENABLE
	if (load == 1) {
		if (ftl->load != NULL) {
			if (ftl->load (bdi, "/usr/share/bdbm_drv/ftl.dat") != 0) {
				bdbm_msg ("loading 'ftl.dat' failed");
				/*goto fail;*/
			}
		} else {
			/*goto fail;*/
		}
	}
#endif

	/* create a high-level memory manager */
	hlm = bdi->ptr_hlm_inf;
	if (hlm->create (bdi) != 0) {
		bdbm_error ("failed to create hlm");
		goto fail;
	}

	/* create a host interface */
	host = bdi->ptr_host_inf;
	if (host->open (bdi) != 0) {
		bdbm_error ("failed to open a host interface");
		goto fail;
	}

	bdbm_msg ("[blueDBM is registered]");

#ifdef BOARD_BSIM
	/*bdbm_nandsim_thread_init ();*/
#endif

	return 0;

fail:
	if (host != NULL)
		host->close (bdi);
	if (hlm != NULL)
		hlm->destroy (bdi);
	if (ftl != NULL)
		ftl->destroy (bdi);
	if (llm != NULL)
		llm->destroy (bdi);
	if (dm != NULL)
		dm->close (bdi);
	if (bdi != NULL)
		bdbm_free_atomic (bdi);

	return -ENXIO;
}

static void __exit bdbm_drv_exit(void)
{
	if (_bdi == NULL)
		return;

	/* display performance results */
	pmu_display (_bdi);
	pmu_destory (_bdi);

#ifdef BOARD_BSIM
	/*bdbm_nandsim_thread_cleanup ();*/
#endif

	if (_bdi->ptr_host_inf != NULL)
		_bdi->ptr_host_inf->close (_bdi);

	if (_bdi->ptr_hlm_inf != NULL)
		_bdi->ptr_hlm_inf->destroy (_bdi);

	if (_bdi->ptr_ftl_inf != NULL)
#ifdef SNAPSHOT_ENABLE
		if (_bdi->ptr_ftl_inf->store)
			_bdi->ptr_ftl_inf->store (_bdi, "/usr/share/bdbm_drv/ftl.dat");
#endif
		_bdi->ptr_ftl_inf->destroy (_bdi);

	if (_bdi->ptr_llm_inf != NULL)
		_bdi->ptr_llm_inf->destroy (_bdi);

	if (_bdi->ptr_dm_inf != NULL) {
#ifdef SNAPSHOT_ENABLE
		if (_bdi->ptr_dm_inf->store)
			_bdi->ptr_dm_inf->store (_bdi, "/usr/share/bdbm_drv/dm.dat");
#endif
		_bdi->ptr_dm_inf->close (_bdi);
	}

	bdbm_free_atomic (_bdi);

#ifdef RAM_CONSUMER
	bdbm_free (ptr_ram_consumer);
#endif

	bdbm_msg ("[blueDBM is removed]");
}

MODULE_AUTHOR ("Sungjin Lee <chamdoo@csail.mit.edu>");
MODULE_DESCRIPTION ("BlueDBM Device Driver");
MODULE_LICENSE ("GPL");

module_init (bdbm_drv_init);
module_exit (bdbm_drv_exit);