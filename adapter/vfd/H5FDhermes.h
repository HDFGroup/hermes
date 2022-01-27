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

#define H5FD_HERMES_NAME  "hermes"
#define H5FD_HERMES_VALUE ((H5FD_class_value_t)(513))

#ifdef __cplusplus
extern "C" {
#endif

hid_t H5FD_hermes_init(void);
herr_t H5Pset_fapl_hermes(hid_t fapl_id, hbool_t persistence, size_t page_size);

#ifdef __cplusplus
}
#endif

#endif /* end H5FDhermes_H */
