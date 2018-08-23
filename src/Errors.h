//===-- src/Errors.h --------*- C++
//-*-===//
//
//
//===----------------------------------------------------------------------===//
///
/// \file This file defines the different success and failure status that the
/// StreamMaster and the Streamer can incour. These error object have some
/// utility methods that can be used to test the more common situations. The
/// Err2Str funciton translate the error status in a human readable string.
///
//===----------------------------------------------------------------------===//

#pragma once
#include <string>

namespace FileWriter {
namespace Status {

//------------------------------------------------------------------------------
/// \brief      Class that helps the StreamMaster to define its status.
///
class StreamMasterError {
  friend const std::string Err2Str(const StreamMasterError &);

public:
  static StreamMasterError OK();
  static StreamMasterError NOT_STARTED();
  static StreamMasterError RUNNING();
  static StreamMasterError HAS_FINISHED();
  static StreamMasterError EMPTY_STREAMER();
  static StreamMasterError IS_REMOVABLE();
  static StreamMasterError STREAMER_ERROR();
  static StreamMasterError REPORT_ERROR();
  static StreamMasterError STREAMMASTER_ERROR();

  //----------------------------------------------------------------------------
  /// \brief      Determines if the streams and the report are closed.
  ///
  /// \return     True if has finished, False otherwise.
  ///
  bool hasFinished() const { return Value == 2; }
  //----------------------------------------------------------------------------
  /// \brief      Determines if the Master can safely remove and destroy the
  /// current StreamMaster.
  ///
  /// \return     True if removable, False otherwise.
  ///
  bool isRemovable() const { return Value == 4; }
  //----------------------------------------------------------------------------
  /// \brief      Determines if the StreamMaster is in a non-failure status.
  ///
  /// \return     True if ok, False otherwise.
  ///
  bool isOK() const { return Value > 0; }
  //----------------------------------------------------------------------------
  /// \brief      Compare two different StreamMasterStatus
  ///
  /// \param[in]  Other  The other.
  ///
  /// \return     True if the status is the same, False otherwise.
  ///
  bool operator==(const StreamMasterError &Other) {
    return Value == Other.Value;
  }
  bool operator!=(const StreamMasterError &Other) {
    return Value == Other.Value;
  }

private:
  int Value{0};
};

//------------------------------------------------------------------------------
/// \brief      Class that helps the Streamer to define its status.
///
enum class StreamerStatus {
  OK = 1000,
  WRITING = 2,
  HAS_FINISHED = 1,
  IS_CONNECTED = 0,
  NOT_INITIALIZED = -1000,
  CONFIGURATION_ERROR = -1,
  TOPIC_PARTITION_ERROR = -2,
  UNKNOWN_ERROR = -1001
};

//------------------------------------------------------------------------------
/// \brief      Converts a StreamerError status into a human readable string.
///
/// \param[in]  Error  The error status.
///
/// \return     The string that briefly describe the status.
///
const std::string Err2Str(const StreamerStatus &Error);

//------------------------------------------------------------------------------
/// \brief      Converts a StreamMasterError status into a human readable
/// string.
///
/// \param[in]  Error  The error status.
///
/// \return     The string that briefly describe the status.
///
const std::string Err2Str(const StreamMasterError &Error);

} // namespace Status
} // namespace FileWriter
