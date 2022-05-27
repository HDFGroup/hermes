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
 *              April 2021
 *
 * Purpose: The public header file for the Hermes driver.
 */
#ifndef H5FDhermes_H
#define H5FDhermes_H

#include <dlfcn.h>
#include <hdf5.h>
#include <stdio.h>

#define H5FD_HERMES_NAME  "hermes"
#define H5FD_HERMES_VALUE ((H5FD_class_value_t)(513))

#define HERMES_FORWARD_DECL(func_, ret_, args_) \
  typedef ret_(*real_t_##func_##_) args_;       \
  ret_(*real_##func_##_) args_ = NULL;

#define MAP_OR_FAIL(func_)                                                  \
  if (!(real_##func_##_)) {                                                 \
    real_##func_##_ = (real_t_##func_##_)dlsym(RTLD_NEXT, #func_);          \
    if (!(real_##func_##_)) {                                               \
      fprintf(stderr, "HERMES Adapter failed to map symbol: %s\n", #func_); \
      exit(1);                                                              \
    }                                                                       \
  }

#ifdef __cplusplus
extern "C" {
#endif

hid_t H5FD_hermes_init();
herr_t H5Pset_fapl_hermes(hid_t fapl_id, hbool_t persistence, size_t page_size);

HERMES_FORWARD_DECL(H5_init_library, herr_t, ());
HERMES_FORWARD_DECL(H5_term_library, herr_t, ());

HERMES_FORWARD_DECL(MPI_Init, int, (int *argc, char ***argv));
HERMES_FORWARD_DECL(MPI_Finalize, int, (void));

#ifdef __cplusplus
}
#endif

#endif /* end H5FDhermes_H */
