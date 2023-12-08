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
#ifndef _GNU_SOURCE
  #define _GNU_SOURCE
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <math.h>
#include <errno.h>

#include <unistd.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h>

/* HDF5 header for dynamic plugin loading */
#include "H5PLextern.h"
#include "H5FDhermes.h"     /* Hermes file driver     */

#include "hermes_adapters/posix/posix_fs_api.h"

/**
 * Make this adapter use Hermes.
 * Disabling will use POSIX.
 * */
#define USE_HERMES

/* The driver identification number, initialized at runtime */
static hid_t H5FD_HERMES_g = H5I_INVALID_HID;

/* Identifiers for HDF5's error API */
hid_t H5FDhermes_err_stack_g = H5I_INVALID_HID;
hid_t H5FDhermes_err_class_g = H5I_INVALID_HID;

/* File operations */
#define OP_UNKNOWN 0
#define OP_READ    1
#define OP_WRITE   2

using hermes::adapter::fs::AdapterStat;
using hermes::adapter::fs::File;
using hermes::adapter::fs::IoStatus;

/* POSIX I/O mode used as the third parameter to open/_open
 * when creating a new file (O_CREAT is set). */
#if defined(H5_HAVE_WIN32_API)
#define H5FD_HERMES_POSIX_CREATE_MODE_RW (_S_IREAD | _S_IWRITE)
#else
#define H5FD_HERMES_POSIX_CREATE_MODE_RW 0666
#endif

#define MAXADDR          (((haddr_t)1 << (8 * sizeof(off_t) - 1)) - 1)
#define SUCCEED 0
#define FAIL    (-1)

#ifdef __cplusplus
extern "C" {
#endif

/* The description of a file/bucket belonging to this driver. */
typedef struct H5FD_hermes_t {
  H5FD_t         pub;         /* public stuff, must be first           */
  haddr_t        eoa;         /* end of allocated region               */
  haddr_t        eof;         /* end of file; current file size        */
  haddr_t        pos;         /* current file I/O position             */
  int            op;          /* last operation                        */
  hbool_t        persistence; /* write to file name on close           */
  int            fd;          /* the filesystem file descriptor        */
  char           *filename_;  /* the name of the file */
  unsigned       flags;       /* The flags passed from H5Fcreate/H5Fopen */
} H5FD_hermes_t;

/* Driver-specific file access properties */
typedef struct H5FD_hermes_fapl_t {
} H5FD_hermes_fapl_t;

/* Prototypes */
static herr_t H5FD__hermes_term(void);
static H5FD_t *H5FD__hermes_open(const char *name, unsigned flags,
                                 hid_t fapl_id, haddr_t maxaddr);
static herr_t H5FD__hermes_close(H5FD_t *_file);
static int H5FD__hermes_cmp(const H5FD_t *_f1, const H5FD_t *_f2);
static herr_t H5FD__hermes_query(const H5FD_t *_f1, unsigned long *flags);
static haddr_t H5FD__hermes_get_eoa(const H5FD_t *_file, H5FD_mem_t type);
static herr_t H5FD__hermes_set_eoa(H5FD_t *_file, H5FD_mem_t type,
                                   haddr_t addr);
static haddr_t H5FD__hermes_get_eof(const H5FD_t *_file, H5FD_mem_t type);
static herr_t H5FD__hermes_read(H5FD_t *_file, H5FD_mem_t type, hid_t fapl_id,
                                haddr_t addr, size_t size, void *buf);
static herr_t H5FD__hermes_write(H5FD_t *_file, H5FD_mem_t type, hid_t fapl_id,
                                 haddr_t addr, size_t size, const void *buf);


static const H5FD_class_t H5FD_hermes_g = {
  H5FD_CLASS_VERSION,      /* struct version       */
  H5FD_HERMES_VALUE,         /* value                */
  H5FD_HERMES_NAME,          /* name                 */
  MAXADDR,                   /* maxaddr              */
  H5F_CLOSE_STRONG,          /* fc_degree            */
  H5FD__hermes_term,         /* terminate            */
  NULL,                      /* sb_size              */
  NULL,                      /* sb_encode            */
  NULL,                      /* sb_decode            */
  0,                         /* fapl_size            */
  NULL,                      /* fapl_get             */
  NULL,                      /* fapl_copy            */
  NULL,                      /* fapl_free            */
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
  NULL,                      /* read_vector          */
  NULL,                      /* write_vector         */
  NULL,                      /* read_selection       */
  NULL,                      /* write_selection      */
  NULL,                      /* flush                */
  NULL,                      /* truncate             */
  NULL,                      /* lock                 */
  NULL,                      /* unlock               */
  NULL,                      /* del                  */
  NULL,                      /* ctl                  */
  H5FD_FLMAP_DICHOTOMY       /* fl_map               */
};

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

  if (H5I_VFL != H5Iget_type(H5FD_HERMES_g)) {
    H5FD_HERMES_g = H5FDregister(&H5FD_hermes_g);
  }

  /* Set return value */
  ret_value = H5FD_HERMES_g;
  return ret_value;
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

  /* Unregister from HDF5 error API */
  if (H5FDhermes_err_class_g >= 0) {
    if (H5Eunregister_class(H5FDhermes_err_class_g) < 0) {
      // TODO(llogan)
    }

    /* Destroy the error stack */
    if (H5Eclose_stack(H5FDhermes_err_stack_g) < 0) {
      // TODO(llogan)
    } /* end if */

    H5FDhermes_err_stack_g = H5I_INVALID_HID;
    H5FDhermes_err_class_g = H5I_INVALID_HID;
  }

  /* Reset VFL ID */
  H5FD_HERMES_g = H5I_INVALID_HID;

  // TODO(llogan): Probably should add back at some point.
  // HERMES->Finalize();
  return ret_value;
} /* end H5FD__hermes_term() */

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
  int fd = -1;
  int o_flags = 0;

  /* Build the open flags */
  o_flags = (H5F_ACC_RDWR & flags) ? O_RDWR : O_RDONLY;
  if (H5F_ACC_TRUNC & flags) {
    o_flags |= O_TRUNC;
  }
  if (H5F_ACC_CREAT & flags) {
    o_flags |= O_CREAT;
  }
  if (H5F_ACC_EXCL & flags) {
    o_flags |= O_EXCL;
  }

#ifdef USE_HERMES
  auto fs_api = HERMES_POSIX_FS;
  bool stat_exists;
  AdapterStat stat;
  stat.flags_ = o_flags;
  stat.st_mode_ = H5FD_HERMES_POSIX_CREATE_MODE_RW;
  File f = fs_api->Open(stat, name);
  fd = f.hermes_fd_;
  HILOG(kDebug, "")
#else
  fd = open(name, o_flags);
#endif
  if (fd < 0) {
    // int myerrno = errno;
    return nullptr;
  }

  /* Create the new file struct */
  file = (H5FD_hermes_t*)calloc(1, sizeof(H5FD_hermes_t));
  if (file == NULL) {
    // TODO(llogan)
  }

  /* Pack file */
  if (name && *name) {
    file->filename_ = strdup(name);
  }
  file->fd = fd;
  file->op = OP_UNKNOWN;
  file->flags = flags;

#ifdef USE_HERMES
  file->eof = (haddr_t)fs_api->GetSize(f, stat_exists);
#else
  file->eof = stdfs::file_size(name);
#endif


  return (H5FD_t *)file;
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
  herr_t ret_value = SUCCEED; /* Return value */
  assert(file);
#ifdef USE_HERMES
  auto fs_api = HERMES_POSIX_FS;
  File f; f.hermes_fd_ = file->fd;
  bool stat_exists;
  fs_api->Close(f, stat_exists);
  HILOG(kDebug, "")
#else
  close(file->fd);
#endif
  if (file->filename_) {
    free(file->filename_);
  }
  free(file);
  return ret_value;
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

  ret_value = strcmp(f1->filename_, f2->filename_);

  return ret_value;
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

  return ret_value;
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
                                    H5FD_mem_t type) {
  (void) type;
  haddr_t ret_value = HADDR_UNDEF;

  const H5FD_hermes_t *file = (const H5FD_hermes_t *)_file;

  ret_value = file->eoa;

  return ret_value;
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
                                   H5FD_mem_t type,
                                   haddr_t addr) {
  (void) type;
  herr_t ret_value = SUCCEED;

  H5FD_hermes_t *file = (H5FD_hermes_t *)_file;

  file->eoa = addr;

  return ret_value;
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
                                    H5FD_mem_t type) {
  (void) type;
  haddr_t ret_value = HADDR_UNDEF;

  const H5FD_hermes_t *file = (const H5FD_hermes_t *)_file;

  ret_value = file->eof;

  return ret_value;
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
static herr_t H5FD__hermes_read(H5FD_t *_file, H5FD_mem_t type,
                                hid_t dxpl_id, haddr_t addr,
                                size_t size, void *buf) {
  (void) dxpl_id; (void) type;
  H5FD_hermes_t *file = (H5FD_hermes_t *)_file;
  herr_t ret_value = SUCCEED;

#ifdef USE_HERMES
  bool stat_exists;
  auto fs_api = HERMES_POSIX_FS;
  File f; f.hermes_fd_ = file->fd; IoStatus io_status;
  size_t count = fs_api->Read(f, stat_exists, buf, addr, size, io_status);
  HILOG(kDebug, "")
#else
  size_t count = read(file->fd, (char*)buf + addr, size);
#endif

  if (count < size) {
    // TODO(llogan)
  }
  return ret_value;
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
static herr_t H5FD__hermes_write(H5FD_t *_file, H5FD_mem_t type,
                                 hid_t dxpl_id, haddr_t addr,
                                 size_t size, const void *buf) {
  (void) dxpl_id; (void) type;
  H5FD_hermes_t *file = (H5FD_hermes_t *)_file;
  herr_t ret_value = SUCCEED;
#ifdef USE_HERMES
  bool stat_exists;
  auto fs_api = HERMES_POSIX_FS;
  File f; f.hermes_fd_ = file->fd; IoStatus io_status;
  size_t count = fs_api->Write(f, stat_exists, buf, addr, size, io_status);
  HILOG(kDebug, "")
#else
  size_t count = write(file->fd, (char*)buf + addr, size);
#endif
  if (count < size) {
    // TODO(llogan)
  }
  return ret_value;
} /* end H5FD__hermes_write() */

/*
 * Stub routines for dynamic plugin loading
 */
H5PL_type_t
H5PLget_plugin_type(void) {
  TRANSPARENT_HERMES();
  return H5PL_TYPE_VFD;
}

const void*
H5PLget_plugin_info(void) {
  TRANSPARENT_HERMES();
  return &H5FD_hermes_g;
}

/** Initialize Hermes */
/*static __attribute__((constructor(101))) void init_hermes_in_vfd(void) {
  std::cout << "IN VFD" << std::endl;
  TRANSPARENT_HERMES;
}*/

}  // extern C
