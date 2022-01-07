/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of the HDF5 HERMES Virtual File Driver. The full        * 
 * copyright notice, including terms governing use, modification, and        *
 * redistribution, is contained in the COPYING file, which can be found at   *
 * the root of the source code distribution tree.                            *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 * Error handling for the HERMES VFD
 */

#ifndef H5FDhermes_err_h
#define H5FDhermes_err_h

#ifdef __cplusplus
extern "C" {
#endif

#include <errno.h>

#include "H5Epublic.h"

extern hid_t H5FDhermes_err_stack_g;
extern hid_t H5FDhermes_err_class_g;

#define H5FD_HERMES_ERR_CLS_NAME "HDF5 HERMES VFD"
#define H5FD_HERMES_ERR_LIB_NAME "HDF5 HERMES VFD"
#define H5FD_HERMES_ERR_VER      "0.1.0"

#define SUCCEED 0
#define FAIL    (-1)

#ifndef FALSE
  #define FALSE false
#endif
#ifndef TRUE
  #define TRUE true
#endif

#define H5_ATTR_UNUSED          __attribute__((unused))

/* Error macros */

/*
 * Macro to push the current function to the current error stack
 * and then goto the "done" label, which should appear inside the
 * function. (compatible with v1 and v2 errors)
 */
#define H5FD_HERMES_GOTO_ERROR(err_major, err_minor, ret_val, ...)      \
  do {                                                                  \
    unsigned is_v2_err;                                                 \
    union {                                                             \
      H5E_auto1_t err_func_v1;                                          \
      H5E_auto2_t err_func_v2;                                          \
    } err_func;                                                         \
                                                                        \
    /* Determine version of error */                                    \
    (void)H5Eauto_is_v2(H5E_DEFAULT, &is_v2_err);                       \
                                                                        \
    if (is_v2_err) {                                                    \
      (void)H5Eget_auto2(H5E_DEFAULT, &err_func.err_func_v2, NULL);     \
    } else {                                                            \
      (void)H5Eget_auto1(&err_func.err_func_v1, NULL);                  \
    }                                                                   \
    /* Check whether automatic error reporting has been disabled */     \
    if (  (is_v2_err && err_func.err_func_v2) ||                        \
          (!is_v2_err && err_func.err_func_v1) ) {                      \
      if (H5FDhermes_err_stack_g >= 0 && H5FDhermes_err_class_g >= 0) { \
        H5Epush2(H5FDhermes_err_stack_g, __FILE__, __func__, __LINE__,  \
                 H5FDhermes_err_class_g, err_major, err_minor, __VA_ARGS__); \
      } else {                                                          \
        fprintf(stderr, __VA_ARGS__);                                   \
        fprintf(stderr, "\n");                                          \
      }                                                                 \
    }                                                                   \
                                                                        \
    ret_value = ret_val;                                                \
    goto done;                                                          \
  } while (0)

/*
 * Macro to push the current function to the current error stack
 * without calling goto. This is used for handling the case where
 * an error occurs during cleanup past the "done" label inside a
 * function so that an infinite loop does not occur where goto
 * continually branches back to the label. (compatible with v1
 * and v2 errors)
 */
#define H5FD_HERMES_DONE_ERROR(err_major, err_minor, ret_val, ...)      \
  do {                                                                  \
    unsigned is_v2_err;                                                 \
    union {                                                             \
      H5E_auto1_t err_func_v1;                                          \
      H5E_auto2_t err_func_v2;                                          \
    } err_func;                                                         \
                                                                        \
    /* Determine version of error */                                    \
    (void)H5Eauto_is_v2(H5E_DEFAULT, &is_v2_err);                       \
                                                                        \
    if (is_v2_err) {                                                    \
      (void)H5Eget_auto2(H5E_DEFAULT, &err_func.err_func_v2, NULL);     \
    } else {                                                            \
      (void)H5Eget_auto1(&err_func.err_func_v1, NULL);                  \
    }                                                                   \
    /* Check whether automatic error reporting has been disabled */     \
    if (  (is_v2_err && err_func.err_func_v2) ||                        \
          (!is_v2_err && err_func.err_func_v1) ) {                      \
      if (H5FDhermes_err_stack_g >= 0 && H5FDhermes_err_class_g >= 0) { \
        H5Epush2(H5FDhermes_err_stack_g, __FILE__, __func__, __LINE__,  \
                 H5FDhermes_err_class_g, err_major, err_minor, __VA_ARGS__); \
      } else {                                                          \
        fprintf(stderr, __VA_ARGS__);                                   \
        fprintf(stderr, "\n");                                          \
      }                                                                 \
    }                                                                   \
                                                                        \
    ret_value = ret_val;                                                \
  } while (0)

/*
 * Macro to print out the current error stack and then clear it
 * for future use. (compatible with v1 and v2 errors)
 */
#define PRINT_ERROR_STACK                                               \
  do {                                                                  \
    unsigned is_v2_err;                                                 \
    union {                                                             \
      H5E_auto1_t err_func_v1;                                          \
      H5E_auto2_t err_func_v2;                                          \
    } err_func;                                                         \
                                                                        \
    /* Determine version of error */                                    \
    (void)H5Eauto_is_v2(H5E_DEFAULT, &is_v2_err);                       \
                                                                        \
    if (is_v2_err) {                                                    \
      (void)H5Eget_auto2(H5E_DEFAULT, &err_func.err_func_v2, NULL);     \
    } else {                                                            \
      (void)H5Eget_auto1(&err_func.err_func_v1, NULL);                  \
    }                                                                   \
    /* Check whether automatic error reporting has been disabled */     \
    if (  (is_v2_err && err_func.err_func_v2) ||                        \
          (!is_v2_err && err_func.err_func_v1) ) {                      \
      if ((H5FDhermes_err_stack_g >= 0) &&                              \
          (H5Eget_num(H5FDhermes_err_stack_g) > 0)) {                   \
        H5Eprint2(H5FDhermes_err_stack_g, NULL);                        \
        H5Eclear2(H5FDhermes_err_stack_g);                              \
      }                                                                 \
    }                                                                   \
  } while (0)

#define H5FD_HERMES_SYS_GOTO_ERROR(err_major, err_minor, ret_val, str)  \
  do {                                                                  \
    int myerrno = errno;                                                \
    H5FD_HERMES_GOTO_ERROR(err_major, err_minor, ret_val,               \
                           "%s, errno = %d, error message = '%s'", str, \
                           myerrno, strerror(myerrno));                 \
  } while (0)

/*
 * Macro to simply jump to the "done" label inside the function,
 * setting ret_value to the given value. This is often used for
 * short circuiting in functions when certain conditions arise.
 */
#define H5FD_HERMES_GOTO_DONE(ret_val)          \
  do {                                          \
    ret_value = ret_val;                        \
    goto done;                                  \
  } while (0)

/*
 * Macro to return from a top-level API function, printing
 * out the error stack on the way out.
 * It should be ensured that this macro is only called once
 * per HDF5 operation. If it is called multiple times per
 * operation (e.g. due to calling top-level API functions
 * internally), the error stack will be inconsistent/incoherent.
 */
#define H5FD_HERMES_FUNC_LEAVE_API              \
  do {                                          \
    PRINT_ERROR_STACK;                          \
    return ret_value;                           \
  } while (0)

/*
 * Macro to return from internal functions.
 */
#define H5FD_HERMES_FUNC_LEAVE                  \
  do {                                          \
    return ret_value;                           \
  } while (0)

#ifdef __cplusplus
}
#endif

#endif /* H5FDhermes_err_h */
