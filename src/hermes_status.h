#ifndef HERMES_STATUS_H_
#define HERMES_STATUS_H_

#include <iostream>
namespace hermes {

#define RETURN_CODES(X)                                          \
  X(2,   HERMES_OK_MAX,              "Maximum supported HERMES success with " \
                                     "caveat.") \
  X(1,   BLOB_IN_SWAP_PLACE,         "Blob is placed into swap place.") \
  X(0,   HERMES_SUCCESS,             "No error!")         \
  X(-1,  INVALID_BUCKET,             "Bucket ID is invalid.") \
  X(-2,  BUCKET_NAME_TOO_LONG,       "Bucket name exceeds max length (256).") \
  X(-3,  VBUCKET_NAME_TOO_LONG,      "VBucket name exceeds max length (256).") \
  X(-4,  BLOB_NAME_TOO_LONG,         "Blob name exceeds max length (64).") \
  X(-5,  INVALID_BLOB,               "Blob data is invalid. Please check " \
                                     "address and size.") \
  X(-6,  BLOB_NOT_IN_BUCKET,         "Blob is not in this bucket.") \
  X(-7,  BLOB_NOT_LINKED_TO_VBUCKET, "Blob is not linked to this vbucket.") \
  X(-8,  TRAIT_NOT_VALID,            "Selected trait is not valid.") \
  X(-9,  TRAIT_EXISTS_ALREADY,       "Selected trait already exists.") \
  X(-10, OFFSET_MAP_EMPTY,           "Offset_map is empty.") \
  X(-11, BLOB_NOT_LINKED_IN_MAP,     "Map doesnt have the blob linked.") \
  X(-12, BUCKET_IN_USE,              "Bucket cannot be destroyed because its "\
                                     "reference count is greater than 1.") \
  X(-13, DPE_RANDOM_FOUND_NO_TGT,    "DPE random found no target with enough " \
                                     "space for blob.") \
  X(-14, DPE_GET_INVALID_TGT,        "DPE got an invalid target ID.") \
  X(-15, DPE_ORTOOLS_NO_SOLUTION,    "DPE or-tools does not find a solution" \
                                     "with provided constraints.") \
  X(-16, DPE_PLACEMENTSCHEMA_EMPTY,  "DPE PlacementSchema is empty.") \
  X(-17, READ_BLOB_FAILED,           "Read blob from its id failed.") \
  X(-18, STDIO_OFFSET_ERROR,         "Offset invalid or fseek() failed.") \
  X(-19, STDIO_FWRITE_FAILED,        "Func fwrite failed. System err msg: ") \
  X(-20, STDIO_FOPEN_FAILED,         "Func fopen failed. System err msg: ") \
  X(-21, STDIO_FCLOSE_FAILED,        "Func fclose failed. System err msg: ") \
  X(-22, FCLOSE_FAILED,              "Func fclose failed. System err msg: ") \
  X(-23, INVALID_FILE,               "File is not valid.") \
  X(-24, PLACE_SWAP_BLOB_TO_BUF_FAILED, "Place blob from swap space into " \
                                     "buffering system failed.") \
  X(-25, DPE_RR_FIND_TGT_FAILED,     "DPE round-robin failed to find " \
                                     "proper target for blob.") \
  X(-26, HERMES_ERROR_MAX,           "Maximum supported HERMES errors.") \

#define RETURN_ENUM(ID, NAME, TEXT) NAME = ID,
#define RETURN_TEXT(ID, NAME, TEXT) case ID: return TEXT;

enum StatusCode {
    RETURN_CODES(RETURN_ENUM)
};

namespace api {

class Status {
 public:
  /** Create an object representing success status. */
  explicit Status(StatusCode ret_code = HERMES_SUCCESS) : status_(ret_code) {}

  /** Returns true if the call did exactly what the user expected */
  bool Succeeded() const {
    return (status_ == HERMES_SUCCESS);
  }

  /** Returns true if the call was success with caveat. */
  bool Acceptable() const {
    return (status_ > HERMES_SUCCESS && status_ < HERMES_OK_MAX);
  }

  /** Returns true if the call failed with error. */
  bool Failed() const {
    return (status_ < HERMES_SUCCESS && status_ > HERMES_ERROR_MAX);
  }

  bool operator == (StatusCode code) {
    return (status_ == code);
  }

  void operator = (StatusCode code) {
    status_ = code;
  }

  StatusCode GetStatus() const {return status_;}

  /** Returns  message */
  std::string Msg() const {
    switch (status_) {
      RETURN_CODES(RETURN_TEXT)
    }

    return "Unknown error";
  }

 private:
  StatusCode status_;
};

}  // namespace api

}  // namespace hermes
#endif  // HERMES_STATUS_H_
