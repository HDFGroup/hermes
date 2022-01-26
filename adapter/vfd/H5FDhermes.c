/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Board of Trustees of the University of Illinois.         *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://www.hdfgroup.org/licenses.               *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 * Programmer:  Kimmy Mu
 *              March 2021
 *
 * Purpose: The hermes file driver using only the HDF5 public API
 *          and buffer datasets in Hermes buffering systems with
 *          multiple storage tiers.
 */

#include <stdio.h>
#include <string.h>
#include <assert.h>

#include <unistd.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h>

/* HDF5 header for dynamic plugin loading */
#include "H5PLextern.h"

#include "H5FDhermes.h"     /* Hermes file driver     */
#include "H5FDhermes_err.h" /* error handling         */

/* Necessary hermes headers */
#include "hermes_wrapper.h"

#define H5FD_HERMES (H5FD_hermes_init())

/* HDF5 doesn't currently have a driver init callback. Use
 * macro to initialize driver if loaded as a plugin.
 */
#define H5FD_HERMES_INIT                        \
  do {                                          \
    if (H5FD_HERMES_g < 0)                      \
      H5FD_HERMES_g = H5FD_HERMES;              \
  } while (0)

/* The driver identification number, initialized at runtime */
static hid_t H5FD_HERMES_g = H5I_INVALID_HID;

/* Identifiers for HDF5's error API */
hid_t H5FDhermes_err_stack_g = H5I_INVALID_HID;
hid_t H5FDhermes_err_class_g = H5I_INVALID_HID;

/* Whether Hermes is initialized */
static htri_t hermes_initialized = FAIL;

/* File operations */
#define OP_UNKNOWN 0
#define OP_READ    1
#define OP_WRITE   2

/* POSIX I/O mode used as the third parameter to open/_open
 * when creating a new file (O_CREAT is set). */
#if defined(H5_HAVE_WIN32_API)
#define H5FD_HERMES_POSIX_CREATE_MODE_RW (_S_IREAD | _S_IWRITE)
#else
#define H5FD_HERMES_POSIX_CREATE_MODE_RW 0666
#endif

/* Define length of blob name, which is converted from page index */
#define LEN_BLOB_NAME 10

#define BIT_SIZE_OF_UNSIGNED (sizeof(uint)*8)

/* kHermesConf env variable is used to define path to kHermesConf in adapters.
 * This is used for initialization of Hermes. */
const char *kHermesConf = "HERMES_CONF";

/* The description of bit representation of blobs in Hermes buffering system. */
typedef struct bitv_t {
  uint  *blobs;
  size_t capacity;
  size_t  end_pos;
} bitv_t;

/* The description of a file/bucket belonging to this driver. */
typedef struct H5FD_hermes_t {
  H5FD_t         pub;         /* public stuff, must be first           */
  haddr_t        eoa;         /* end of allocated region               */
  haddr_t        eof;         /* end of file; current file size        */
  haddr_t        pos;         /* current file I/O position             */
  int            op;          /* last operation                        */
  hbool_t        persistence; /* write to file name on close           */
  int            fd;          /* the filesystem file descriptor        */
  size_t         buf_size;
  char          *bktname;     /* Copy of file name from open operation */
  BucketClass   *bkt_handle;
  int            ref_count;
  unsigned char *page_buf;
  bitv_t         blob_in_bucket;
} H5FD_hermes_t;

/* Driver-specific file access properties */
typedef struct H5FD_hermes_fapl_t {
  hbool_t persistence; /* write to file name on flush */
  size_t  page_size;   /* page size */
} H5FD_hermes_fapl_t;

/*
 * These macros check for overflow of various quantities.  These macros
 * assume that HDoff_t is signed and haddr_t and size_t are unsigned.
 *
 * ADDR_OVERFLOW:   Checks whether a file address of type `haddr_t'
 *                  is too large to be represented by the second argument
 *                  of the file seek function.
 *
 * SIZE_OVERFLOW:   Checks whether a buffer size of type `hsize_t' is too
 *                  large to be represented by the `size_t' type.
 *
 * REGION_OVERFLOW: Checks whether an address and size pair describe data
 *                  which can be addressed entirely by the second
 *                  argument of the file seek function.
 */
#define MAXADDR          (((haddr_t)1 << (8 * sizeof(off_t) - 1)) - 1)
#define ADDR_OVERFLOW(A) (HADDR_UNDEF == (A) || ((A) & ~(haddr_t)MAXADDR))
#define SIZE_OVERFLOW(Z) ((Z) & ~(hsize_t)MAXADDR)
#define REGION_OVERFLOW(A, Z)                                           \
  (ADDR_OVERFLOW(A) || SIZE_OVERFLOW(Z) || HADDR_UNDEF == (A) + (Z) ||  \
  (off_t)((A) + (Z)) < (off_t)(A))

/* Prototypes */
static herr_t  H5FD__hermes_term(void);
static herr_t  H5FD__hermes_fapl_free(void *_fa);
static H5FD_t *H5FD__hermes_open(const char *name, unsigned flags,
                                 hid_t fapl_id, haddr_t maxaddr);
static herr_t  H5FD__hermes_close(H5FD_t *_file);
static int     H5FD__hermes_cmp(const H5FD_t *_f1, const H5FD_t *_f2);
static herr_t  H5FD__hermes_query(const H5FD_t *_f1, unsigned long *flags);
static haddr_t H5FD__hermes_get_eoa(const H5FD_t *_file, H5FD_mem_t type);
static herr_t  H5FD__hermes_set_eoa(H5FD_t *_file, H5FD_mem_t type,
                                    haddr_t addr);
static haddr_t H5FD__hermes_get_eof(const H5FD_t *_file, H5FD_mem_t type);
static herr_t  H5FD__hermes_read(H5FD_t *_file, H5FD_mem_t type, hid_t fapl_id,
                                 haddr_t addr, size_t size, void *buf);
static herr_t  H5FD__hermes_write(H5FD_t *_file, H5FD_mem_t type, hid_t fapl_id,
                                  haddr_t addr, size_t size, const void *buf);

static const H5FD_class_t H5FD_hermes_g = {
  H5FD_HERMES_VALUE,         /* value                */
  H5FD_HERMES_NAME,          /* name                 */
  MAXADDR,                   /* maxaddr              */
  H5F_CLOSE_STRONG,          /* fc_degree            */
  H5FD__hermes_term,         /* terminate            */
  NULL,                      /* sb_size              */
  NULL,                      /* sb_encode            */
  NULL,                      /* sb_decode            */
  sizeof(H5FD_hermes_fapl_t),/* fapl_size            */
  NULL,                      /* fapl_get             */
  NULL,                      /* fapl_copy            */
  H5FD__hermes_fapl_free,    /* fapl_free            */
  0,                         /* dxpl_size            */
  NULL,                      /* dxpl_copy            */
  NULL,                      /* dxpl_free            */
  H5FD__hermes_open,         /* open                 */
  H5FD__hermes_close,        /* close                */
  H5FD__hermes_cmp,          /* cmp                  */
  H5FD__hermes_query,        /* query                */
  NULL,                      /* get_type_map         */
  NULL,                      /* alloc                */
  NULL,                      /* free                 */
  H5FD__hermes_get_eoa,      /* get_eoa              */
  H5FD__hermes_set_eoa,      /* set_eoa              */
  H5FD__hermes_get_eof,      /* get_eof              */
  NULL,                      /* get_handle           */
  H5FD__hermes_read,         /* read                 */
  H5FD__hermes_write,        /* write                */
  NULL,                      /* flush                */
  NULL,                      /* truncate             */
  NULL,                      /* lock                 */
  NULL,                      /* unlock               */
  NULL,                      /* del                  */
  NULL,                      /* ctl                  */
  H5FD_FLMAP_DICHOTOMY       /* fl_map               */
};

/* Check if the blob at POS is set */
static bool check_blob(bitv_t *bits, size_t bit_pos) {
  bool result = false;

  if (bit_pos >= bits->capacity)
    return false;

  size_t unit_pos = bit_pos / BIT_SIZE_OF_UNSIGNED;
  size_t blob_pos_in_unit = bit_pos % BIT_SIZE_OF_UNSIGNED;
  result = bits->blobs[unit_pos] & (1 << blob_pos_in_unit);

  return result;
}

/* Set the bit at POS and reallocate 2*capacity for blobs as needed */
static void set_blob(bitv_t *bits, size_t bit_pos) {
  if (bit_pos >= bits->capacity) {
    size_t current_units = bits->capacity/BIT_SIZE_OF_UNSIGNED;
    size_t need_units = bit_pos/BIT_SIZE_OF_UNSIGNED + 1;
    bits->capacity = need_units * BIT_SIZE_OF_UNSIGNED * 2;
    bits->blobs = realloc(bits->blobs, bits->capacity);
    memset(&bits->blobs[current_units], 0,
           sizeof(uint)*(need_units*2-current_units+1));
  }

  size_t unit_pos = bit_pos / BIT_SIZE_OF_UNSIGNED;
  size_t blob_pos_in_unit = bit_pos % BIT_SIZE_OF_UNSIGNED;
  bits->blobs[unit_pos] |= 1 << blob_pos_in_unit;
  if (bit_pos > bits->end_pos)
    bits->end_pos = bit_pos;
}

/*-------------------------------------------------------------------------
 * Function:    H5FD_hermes_init
 *
 * Purpose:     Initialize this driver by registering the driver with the
 *              library.
 *
 * Return:      Success:    The driver ID for the hermes driver
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5FD_hermes_init(void) {
  hid_t ret_value = H5I_INVALID_HID; /* Return value */

  /* Initialize error reporting */
  if ((H5FDhermes_err_stack_g = H5Ecreate_stack()) < 0)
    H5FD_HERMES_GOTO_ERROR(H5E_VFL, H5E_CANTINIT, H5I_INVALID_HID,
                           "can't create HDF5 error stack");
  if ((H5FDhermes_err_class_g = H5Eregister_class(H5FD_HERMES_ERR_CLS_NAME,
                                                  H5FD_HERMES_ERR_LIB_NAME,
                                                  H5FD_HERMES_ERR_VER)) < 0)
    H5FD_HERMES_GOTO_ERROR(H5E_VFL, H5E_CANTINIT, H5I_INVALID_HID,
                           "can't register error class with HDF5 error API");

  if (H5I_VFL != H5Iget_type(H5FD_HERMES_g))
    H5FD_HERMES_g = H5FDregister(&H5FD_hermes_g);

  /* Set return value */
  ret_value = H5FD_HERMES_g;

done:
  H5FD_HERMES_FUNC_LEAVE;
} /* end H5FD_hermes_init() */

/*---------------------------------------------------------------------------
 * Function:    H5FD__hermes_term
 *
 * Purpose:     Shut down the VFD
 *
 * Returns:     SUCCEED (Can't fail)
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5FD__hermes_term(void) {
  herr_t ret_value = SUCCEED;

  if ((H5OPEN hermes_initialized) == TRUE) {
    HermesFinalize();
    hermes_initialized = FALSE;
  }

  /* Unregister from HDF5 error API */
  if (H5FDhermes_err_class_g >= 0) {
    if (H5Eunregister_class(H5FDhermes_err_class_g) < 0)
      H5FD_HERMES_GOTO_ERROR(
        H5E_VFL, H5E_CLOSEERROR, FAIL,
        "can't unregister error class from HDF5 error API");

    /* Print the current error stack before destroying it */
    PRINT_ERROR_STACK;

    /* Destroy the error stack */
    if (H5Eclose_stack(H5FDhermes_err_stack_g) < 0) {
      H5FD_HERMES_GOTO_ERROR(H5E_VFL, H5E_CLOSEERROR, FAIL,
                             "can't close HDF5 error stack");
      PRINT_ERROR_STACK;
    } /* end if */

    H5FDhermes_err_stack_g = H5I_INVALID_HID;
    H5FDhermes_err_class_g = H5I_INVALID_HID;
  }

  /* Reset VFL ID */
  H5FD_HERMES_g = H5I_INVALID_HID;

done:
  H5FD_HERMES_FUNC_LEAVE_API;
} /* end H5FD__hermes_term() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_fapl_hermes
 *
 * Purpose:     Modify the file access property list to use the H5FD_HERMES
 *              driver defined in this source file.  There are no driver
 *              specific properties.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_fapl_hermes(hid_t fapl_id, hbool_t persistence, size_t page_size) {
  H5FD_hermes_fapl_t fa; /* Hermes VFD info */
  herr_t ret_value = SUCCEED; /* Return value */

  /* Check argument */
  if (H5I_GENPROP_LST != H5Iget_type(fapl_id) ||
      TRUE != H5Pisa_class(fapl_id, H5P_FILE_ACCESS)) {
    H5FD_HERMES_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL,
                           "not a file access property list");
  }

  /* Set VFD info values */
  memset(&fa, 0, sizeof(H5FD_hermes_fapl_t));
  fa.persistence  = persistence;
  fa.page_size = page_size;

  /* Set the property values & the driver for the FAPL */
  if (H5Pset_driver(fapl_id, H5FD_HERMES, &fa) < 0) {
    H5FD_HERMES_GOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL,
                           "can't set Hermes VFD as driver");
  }

done:
  H5FD_HERMES_FUNC_LEAVE_API;
}  /* end H5Pset_fapl_hermes() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__hermes_fapl_free
 *
 * Purpose:    Frees the family-specific file access properties.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t H5FD__hermes_fapl_free(void *_fa) {
  H5FD_hermes_fapl_t *fa = (H5FD_hermes_fapl_t *)_fa;
  herr_t ret_value = SUCCEED;  /* Return value */

  free(fa);

  H5FD_HERMES_FUNC_LEAVE;
}

/*-------------------------------------------------------------------------
 * Function:    H5FD__hermes_open
 *
 * Purpose:     Create and/or opens a bucket in Hermes.
 *
 * Return:      Success:    A pointer to a new bucket data structure.
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static H5FD_t *
H5FD__hermes_open(const char *name, unsigned flags, hid_t fapl_id,
                  haddr_t maxaddr) {
  H5FD_hermes_t  *file = NULL; /* hermes VFD info          */
  int             fd   = -1;   /* File descriptor          */
  int             o_flags;     /* Flags for open() call    */
  struct stat     sb;
  const H5FD_hermes_fapl_t *fa   = NULL;
  H5FD_hermes_fapl_t new_fa;
  char           *hermes_config = NULL;
  H5FD_t         *ret_value = NULL; /* Return value */

  /* Sanity check on file offsets */
  assert(sizeof(off_t) >= sizeof(size_t));

  H5FD_HERMES_INIT;

  /* Check arguments */
  if (!name || !*name)
    H5FD_HERMES_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "invalid file name");
  if (0 == maxaddr || HADDR_UNDEF == maxaddr)
    H5FD_HERMES_GOTO_ERROR(H5E_ARGS, H5E_BADRANGE, NULL, "bogus maxaddr");
  if (ADDR_OVERFLOW(maxaddr))
    H5FD_HERMES_GOTO_ERROR(H5E_ARGS, H5E_OVERFLOW, NULL, "bogus maxaddr");

  /* Get the driver specific information */
  H5E_BEGIN_TRY {
    fa = H5Pget_driver_info(fapl_id);
  }
  H5E_END_TRY;
  if (!fa || (H5P_FILE_ACCESS_DEFAULT == fapl_id)) {
    ssize_t config_str_len = 0;
    char config_str_buf[128];
    if ((config_str_len =
         H5Pget_driver_config_str(fapl_id, config_str_buf, 128)) < 0) {
      printf("H5Pget_driver_config_str() error\n");
    }
    char *saveptr = NULL;
    char* token = strtok_r(config_str_buf, " ", &saveptr);
    if (!strcmp(token, "true") || !strcmp(token, "TRUE") ||
        !strcmp(token, "True")) {
      new_fa.persistence = true;
    }
    token = strtok_r(0, " ", &saveptr);
    sscanf(token, "%zu", &(new_fa.page_size));
    fa = &new_fa;
  }

  /* Initialize Hermes */
  if ((H5OPEN hermes_initialized) == FAIL) {
    hermes_config = getenv(kHermesConf);
    if (HermesInitHermes(hermes_config) < 0) {
      H5FD_HERMES_GOTO_ERROR(H5E_SYM, H5E_UNINITIALIZED, NULL,
                             "Hermes initialization failed");
    } else {
      hermes_initialized = TRUE;
    }
  }

  /* Create the new file struct */
  if (NULL == (file = calloc(1, sizeof(H5FD_hermes_t)))) {
    H5FD_HERMES_GOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL,
                           "unable to allocate file struct");
  }

  if (name && *name) {
    file->bktname = strdup(name);
  }
  file->persistence = fa->persistence;
  file->fd = -1;
  file->bkt_handle = HermesBucketCreate(name);
  file->page_buf = malloc(fa->page_size);
  file->buf_size = fa->page_size;
  file->ref_count = 1;
  file->op = OP_UNKNOWN;
  file->blob_in_bucket.capacity = BIT_SIZE_OF_UNSIGNED;
  file->blob_in_bucket.blobs = (uint *)calloc(1, sizeof(uint));
  file->blob_in_bucket.end_pos = 0;

  if (fa->persistence) {
    /* Build the open flags */
    o_flags = (H5F_ACC_RDWR & flags) ? O_RDWR : O_RDONLY;
    if (H5F_ACC_TRUNC & flags)
      o_flags |= O_TRUNC;
    if (H5F_ACC_CREAT & flags)
      o_flags |= O_CREAT;
    if (H5F_ACC_EXCL & flags)
      o_flags |= O_EXCL;

    /* Open the file */
    if ((fd = open(name, o_flags, H5FD_HERMES_POSIX_CREATE_MODE_RW)) < 0) {
      int myerrno = errno;
      H5FD_HERMES_GOTO_ERROR(
        H5E_FILE, H5E_CANTOPENFILE, NULL,
        "unable to open file: name = '%s', errno = %d, error message = '%s',"
        "flags = %x, o_flags = %x", name, myerrno, strerror(myerrno), flags,
        (unsigned)o_flags);
    }

    if (fstat(fd, &sb) < 0) {
      H5FD_HERMES_SYS_GOTO_ERROR(H5E_FILE, H5E_BADFILE, NULL,
                                 "unable to fstat file");
    }

    /* FIXME: Possible overflow! */
    file->eof = (haddr_t)sb.st_size;
    file->fd = fd;
  }

  /* Set return value */
  ret_value = (H5FD_t *)file;

done:
  if (NULL == ret_value) {
    if (fd >= 0)
      close(fd);
    if (file) {
      HermesBucketDestroy(file->bkt_handle);
      free(file->blob_in_bucket.blobs);
      free(file);
    }
  } /* end if */

  H5FD_HERMES_FUNC_LEAVE_API;
} /* end H5FD__hermes_open() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__hermes_close
 *
 * Purpose:     Closes an HDF5 file.
 *
 * Return:      Success:    SUCCEED
 *              Failure:    FAIL, file not closed.
 *
 *-------------------------------------------------------------------------
 */
static herr_t H5FD__hermes_close(H5FD_t *_file) {
  H5FD_hermes_t *file = (H5FD_hermes_t *)_file;
  size_t blob_size = file->buf_size;
  size_t i;
  herr_t ret_value = SUCCEED; /* Return value */

  /* Sanity check */
  assert(file);

  if (file->persistence) {
    if (file->op == OP_WRITE) {
      /* TODO: if there is user blobk, the logic is not working,
         including offset, but not 0 */
      for (i = 0; i <= file->blob_in_bucket.end_pos; i++) {
        /* Check if this blob exists */
        bool blob_exists = check_blob(&file->blob_in_bucket, i);
        if (!blob_exists)
          continue;

        char i_blob[LEN_BLOB_NAME];
        snprintf(i_blob, sizeof(i_blob), "%zu\n", i);
        /* Read blob back */
        HermesBucketGet(file->bkt_handle, i_blob, blob_size, file->page_buf);
        ssize_t bytes_wrote;
        if (i == file->blob_in_bucket.end_pos) {
          size_t bytes_in = file->eof%blob_size;
          if (bytes_in == 0)
            bytes_in = blob_size;
          bytes_wrote = pwrite(file->fd, file->page_buf,
                               bytes_in, i*blob_size);
          assert(bytes_wrote == bytes_in);
        } else {
          bytes_wrote = pwrite(file->fd, file->page_buf,
                               blob_size, i*blob_size);
          assert(bytes_wrote == blob_size);
        }
      }
    }
    if (close(file->fd) < 0)
      H5FD_HERMES_SYS_GOTO_ERROR(H5E_IO, H5E_CANTCLOSEFILE, FAIL,
                                 "unable to close file");
  }
  if (file->ref_count == 1)
    HermesBucketDestroy(file->bkt_handle);
  else
    HermesBucketClose(file->bkt_handle);

  /* Release the file info */
  free(file->blob_in_bucket.blobs);
  free(file);

done:
  H5FD_HERMES_FUNC_LEAVE_API;
} /* end H5FD__hermes_close() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__hermes_cmp
 *
 * Purpose:     Compares two buckets belonging to this driver using an
 *              arbitrary (but consistent) ordering.
 *
 * Return:      Success:    A value like strcmp()
 *              Failure:    never fails (arguments were checked by the
 *                          caller).
 *
 *-------------------------------------------------------------------------
 */
static int H5FD__hermes_cmp(const H5FD_t *_f1, const H5FD_t *_f2) {
  const H5FD_hermes_t *f1        = (const H5FD_hermes_t *)_f1;
  const H5FD_hermes_t *f2        = (const H5FD_hermes_t *)_f2;
  int                  ret_value = 0;

  ret_value = strcmp(f1->bktname, f2->bktname);

  H5FD_HERMES_FUNC_LEAVE;
} /* end H5FD__hermes_cmp() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__hermes_query
 *
 * Purpose:     Set the flags that this VFL driver is capable of supporting.
 *              (listed in H5FDpublic.h)
 *
 * Return:      SUCCEED (Can't fail)
 *
 *-------------------------------------------------------------------------
 */
static herr_t H5FD__hermes_query(const H5FD_t *_file,
                                 unsigned long *flags /* out */) {
  /* Set the VFL feature flags that this driver supports */
  /* Notice: the Mirror VFD Writer currently uses only the hermes driver as
   * the underying driver -- as such, the Mirror VFD implementation copies
   * these feature flags as its own. Any modifications made here must be
   * reflected in H5FDmirror.c
   * -- JOS 2020-01-13
   */
  herr_t ret_value = SUCCEED;

  if (flags) {
    *flags = 0;
  }                                            /* end if */

  H5FD_HERMES_FUNC_LEAVE;
} /* end H5FD__hermes_query() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__hermes_get_eoa
 *
 * Purpose:     Gets the end-of-address marker for the file. The EOA marker
 *              is the first address past the last byte allocated in the
 *              format address space.
 *
 * Return:      The end-of-address marker.
 *
 *-------------------------------------------------------------------------
 */
static haddr_t H5FD__hermes_get_eoa(const H5FD_t *_file,
                                    H5FD_mem_t H5_ATTR_UNUSED type) {
  haddr_t ret_value = HADDR_UNDEF;

  const H5FD_hermes_t *file = (const H5FD_hermes_t *)_file;

  ret_value = file->eoa;

  H5FD_HERMES_FUNC_LEAVE;
} /* end H5FD__hermes_get_eoa() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__hermes_set_eoa
 *
 * Purpose:     Set the end-of-address marker for the file. This function is
 *              called shortly after an existing HDF5 file is opened in order
 *              to tell the driver where the end of the HDF5 data is located.
 *
 * Return:      SUCCEED (Can't fail)
 *
 *-------------------------------------------------------------------------
 */
static herr_t H5FD__hermes_set_eoa(H5FD_t *_file,
                                   H5FD_mem_t H5_ATTR_UNUSED type,
                                   haddr_t addr) {
  herr_t ret_value = SUCCEED;

  H5FD_hermes_t *file = (H5FD_hermes_t *)_file;

  file->eoa = addr;

  H5FD_HERMES_FUNC_LEAVE;
} /* end H5FD__hermes_set_eoa() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__hermes_get_eof
 *
 * Purpose:     Returns the end-of-file marker, which is the greater of
 *              either the filesystem end-of-file or the HDF5 end-of-address
 *              markers.
 *
 * Return:      End of file address, the first address past the end of the
 *              "file", either the filesystem file or the HDF5 file.
 *
 *-------------------------------------------------------------------------
 */
static haddr_t H5FD__hermes_get_eof(const H5FD_t *_file,
                                    H5FD_mem_t H5_ATTR_UNUSED type) {
  haddr_t ret_value = HADDR_UNDEF;

  const H5FD_hermes_t *file = (const H5FD_hermes_t *)_file;

  ret_value = file->eof;

  H5FD_HERMES_FUNC_LEAVE;
} /* end H5FD__hermes_get_eof() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__hermes_read
 *
 * Purpose:     Reads SIZE bytes of data from FILE beginning at address ADDR
 *              into buffer BUF according to data transfer properties in
 *              DXPL_ID. Determine the number of file pages affected by this
 *              call from ADDR and SIZE. Utilize transfer buffer PAGE_BUF to
 *              read the data from Blobs. Exercise care for the first and last
 *              pages to prevent overwriting existing data.
 *
 * Return:      Success:    SUCCEED. Result is stored in caller-supplied
 *                          buffer BUF.
 *              Failure:    FAIL, Contents of buffer BUF are undefined.
 *
 *-------------------------------------------------------------------------
 */
static herr_t H5FD__hermes_read(H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type,
                                hid_t H5_ATTR_UNUSED dxpl_id, haddr_t addr,
                                size_t size, void *buf /*out*/) {
  H5FD_hermes_t *file      = (H5FD_hermes_t *)_file;
  size_t         num_pages; /* Number of pages of transfer buffer */
  size_t         start_page_index; /* First page index of tranfer buffer */
  size_t         end_page_index; /* End page index of tranfer buffer */
  size_t         transfer_size = 0;
  size_t         blob_size = file->buf_size;
  size_t         k;
  haddr_t        addr_end = addr+size-1;
  herr_t         ret_value = SUCCEED; /* Return value */

  assert(file && file->pub.cls);
  assert(buf);

  /* Check for overflow conditions */
  if (HADDR_UNDEF == addr) {
    H5FD_HERMES_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                           "addr undefined, addr = %llu",
                           (unsigned long long)addr);
  }
  if (REGION_OVERFLOW(addr, size)) {
    H5FD_HERMES_GOTO_ERROR(H5E_ARGS, H5E_OVERFLOW, FAIL,
                           "addr overflow, addr = %llu",
                           (unsigned long long)addr);
  }
  if (NULL == file->page_buf) {
    H5FD_HERMES_GOTO_ERROR(H5E_INTERNAL, H5E_UNINITIALIZED, FAIL,
                           "transfer buffer not initialized");
  }

  /* Check easy cases */
  if (0 == size)
    return 0;
  if ((haddr_t)addr >= file->eof) {
    memset(buf, 0, size);
    return 0;
  }

  start_page_index = addr/blob_size;
  end_page_index = addr_end/blob_size;
  num_pages = end_page_index - start_page_index + 1;

  for (k = start_page_index; k <= end_page_index; ++k) {
    size_t bytes_in;
    char k_blob[LEN_BLOB_NAME];
    snprintf(k_blob, sizeof(k_blob), "%zu\n", k);
    /* Check if this blob exists */
    bool blob_exists = check_blob(&file->blob_in_bucket, k);

    /* Check if addr is in the range of (k*blob_size, (k+1)*blob_size) */
    /* NOTE: The range does NOT include the start address of page k,
       but includes the end address of page k */
    if (addr > k*blob_size && addr < (k+1)*blob_size) {
      /* Calculate the starting address of transfer buffer update within page
       * k */
      size_t offset = addr - k*blob_size;
      assert(offset > 0);

      if (addr_end <= (k+1)*blob_size-1)
        bytes_in = size;
      else
        bytes_in = (k+1)*blob_size-addr;

      if (!blob_exists) {
        size_t bytes_copy;
        if (file->eof < (k+1)*blob_size-1)
          bytes_copy = file->eof-k*blob_size;
        else
          bytes_copy = blob_size;

        size_t bytes_read = pread(file->fd, file->page_buf, bytes_copy,
                                  k*blob_size);
        if (bytes_read != bytes_copy)
          H5FD_HERMES_GOTO_ERROR(H5E_IO, H5E_READERROR, FAIL, "pread failed");
        memcpy(buf, file->page_buf+offset, bytes_in);

        /* Write Blob k to Hermes buffering system */
        HermesBucketPut(file->bkt_handle, k_blob, file->page_buf, blob_size);
        set_blob(&file->blob_in_bucket, k);
      } else {
        /* Read blob back to transfer buffer */
        HermesBucketGet(file->bkt_handle, k_blob, blob_size, file->page_buf);

        memcpy(buf, file->page_buf+offset, bytes_in);
      }
      transfer_size += bytes_in;
      /* Check if addr_end is in the range of [k*blob_size,
       * (k+1)*blob_size-1) */
      /* NOTE: The range includes the start address of page k,
         but does NOT include the end address of page k */
    } else if (addr_end >= k*blob_size && addr_end < (k+1)*blob_size-1) {
      bytes_in = addr_end-k*blob_size+1;
      if (!blob_exists) {
        size_t bytes_copy;
        if (file->eof < (k+1)*blob_size-1)
          bytes_copy = file->eof-k*blob_size;
        else
          bytes_copy = blob_size;

        ssize_t bytes_read = pread(file->fd, file->page_buf, bytes_copy,
                                   k*blob_size);
        if (bytes_read != bytes_copy)
          H5FD_HERMES_GOTO_ERROR(H5E_IO, H5E_READERROR, FAIL, "pread failed");

        memcpy(buf+transfer_size, file->page_buf, bytes_in);

        /* Write Blob k to Hermes buffering system */
        HermesBucketPut(file->bkt_handle, k_blob, file->page_buf, blob_size);
        set_blob(&file->blob_in_bucket, k);
      } else {
        /* Read blob back to transfer buffer */
        HermesBucketGet(file->bkt_handle, k_blob, blob_size, file->page_buf);

        /* Update transfer buffer */
        memcpy(buf+transfer_size, file->page_buf, bytes_in);
      }
      transfer_size += bytes_in;
      /* Page/Blob k is within the range of (addr, addr+size) */
      /* addr <= k*blob_size && addr_end >= (k+1)*blob_size-1 */
    } else {
      if (!blob_exists) {
        ssize_t bytes_read = pread(file->fd, buf+transfer_size, blob_size,
                                   addr+transfer_size);
        if (bytes_read != blob_size)
          H5FD_HERMES_GOTO_ERROR(H5E_IO, H5E_READERROR, FAIL, "pread failed");

        /* Write Blob k to Hermes buffering system */
        HermesBucketPut(file->bkt_handle, k_blob, buf+transfer_size, blob_size);
        set_blob(&file->blob_in_bucket, k);
      } else {
        /* Read blob back directly */
        HermesBucketGet(file->bkt_handle, k_blob, blob_size, buf+transfer_size);
      }
      transfer_size += blob_size;
    }
  }

  /* Update current position */
  file->pos = addr+size;
  file->op  = OP_READ;

done:
  if (ret_value < 0) {
    /* Reset last file I/O information */
    file->pos = HADDR_UNDEF;
    file->op  = OP_UNKNOWN;
  } /* end if */

  H5FD_HERMES_FUNC_LEAVE_API;
} /* end H5FD__hermes_read() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__hermes_write
 *
 * Purpose:     Writes SIZE bytes of data contained in buffer BUF to Hermes
 *              buffering system according to data transfer properties in
 *              DXPL_ID. Determine the number of file pages affected by this
 *              call from ADDR and SIZE. Utilize transfer buffer PAGE_BUF to
 *              put the data into Blobs. Exercise care for the first and last
 *              pages to prevent overwriting existing data.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t H5FD__hermes_write(H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type,
                                 hid_t H5_ATTR_UNUSED dxpl_id, haddr_t addr,
                                 size_t size, const void *buf) {
  H5FD_hermes_t *file      = (H5FD_hermes_t *)_file;
  size_t         num_pages; /* Number of pages of transfer buffer */
  size_t         start_page_index; /* First page index of tranfer buffer */
  size_t         end_page_index; /* End page index of tranfer buffer */
  size_t         transfer_size = 0;
  size_t         blob_size = file->buf_size;
  size_t         k;
  haddr_t        addr_end = addr+size-1;
  herr_t         ret_value = SUCCEED; /* Return value */

  assert(file && file->pub.cls);
  assert(buf);

  /* Check for overflow conditions */
  if (HADDR_UNDEF == addr) {
    H5FD_HERMES_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                           "addr undefined, addr = %llu",
                           (unsigned long long)addr);
  }
  if (REGION_OVERFLOW(addr, size)) {
    H5FD_HERMES_GOTO_ERROR(H5E_ARGS, H5E_OVERFLOW, FAIL,
                           "addr overflow, addr = %llu, size = %llu",
                           (unsigned long long)addr, (unsigned long long)size);
  }
  if (NULL == file->page_buf) {
    H5FD_HERMES_GOTO_ERROR(H5E_INTERNAL, H5E_UNINITIALIZED, FAIL,
                           "transfer buffer not initialized");
  }
  start_page_index = addr/blob_size;
  end_page_index = addr_end/blob_size;
  num_pages = end_page_index - start_page_index + 1;

  /* Assume only using one page now */
  for (k = start_page_index; k <= end_page_index; ++k) {
    char k_blob[LEN_BLOB_NAME];
    snprintf(k_blob, sizeof(k_blob), "%zu\n", k);
    /* Check if addr is in the range of (k*blob_size, (k+1)*blob_size) */
    /* NOTE: The range does NOT include the start address of page k,
       but includes the end address of page k */
    if (addr > k*blob_size && addr < (k+1)*blob_size) {
      /* Check if this blob exists */
      bool blob_exists = check_blob(&file->blob_in_bucket, k);
      /* Read blob back to transfer buffer */
      if (blob_exists) {
        HermesBucketGet(file->bkt_handle, k_blob, blob_size, file->page_buf);
      }
      /* Calculate the starting address of transfer buffer update within page
       * k */
      size_t offset = addr - k*blob_size;
      assert(offset > 0);

      /* Update transfer buffer */
      /* addr+size is within the same page (only one page) */
      if (addr_end <= (k+1)*blob_size-1) {
        memcpy(file->page_buf+offset, buf+transfer_size, size);
        transfer_size += size;
      } else {
        /* More than one page */
        /* Copy data from addr to the end of the address in page k */
        memcpy(file->page_buf+offset, buf+transfer_size, (k+1)*blob_size-addr);
        transfer_size += (k+1)*blob_size-addr;
      }
      /* Write Blob k to Hermes buffering system */
      HermesBucketPut(file->bkt_handle, k_blob, file->page_buf, blob_size);
      set_blob(&file->blob_in_bucket, k);
    } else if (addr_end >= k*blob_size && addr_end < (k+1)*blob_size-1) {
      /* Check if addr_end is in the range of [k*blob_size,
       * (k+1)*blob_size-1) */
      /* NOTE: The range includes the start address of page k,
         but does NOT include the end address of page k */
      /* Check if this blob exists */
      bool blob_exists = check_blob(&file->blob_in_bucket, k);
      /* Read blob back */
      if (blob_exists) {
        HermesBucketGet(file->bkt_handle, k_blob, blob_size, file->page_buf);
      }
      /* Update transfer buffer */
      memcpy(file->page_buf, buf+transfer_size, addr_end-k*blob_size+1);
      transfer_size += addr_end-k*blob_size+1;
      /* Write Blob k to Hermes buffering system */
      HermesBucketPut(file->bkt_handle, k_blob, file->page_buf, blob_size);
      set_blob(&file->blob_in_bucket, k);
    } else if (addr <= k*blob_size && addr_end >= (k+1)*blob_size-1) {
      /* Page/Blob k is within the range of (addr, addr+size) */
      /* Update transfer buffer */
      /* Write Blob k to Hermes buffering system */
      HermesBucketPut(file->bkt_handle, k_blob, buf+transfer_size, blob_size);
      set_blob(&(file->blob_in_bucket), k);
      transfer_size += blob_size;
    }
  }

  /* Update current position and eof */
  file->pos = addr+size;
  file->op  = OP_WRITE;
  if (file->pos > file->eof)
    file->eof = file->pos;

done:
  if (ret_value < 0) {
    /* Reset last file I/O information */
    file->pos = HADDR_UNDEF;
    file->op  = OP_UNKNOWN;
  } /* end if */

  H5FD_HERMES_FUNC_LEAVE;
} /* end H5FD__hermes_write() */

/*
 * Stub routines for dynamic plugin loading
 */
H5PL_type_t
H5PLget_plugin_type(void) {
  return H5PL_TYPE_VFD;
}

const void*
H5PLget_plugin_info(void) {
  return &H5FD_hermes_g;
}
