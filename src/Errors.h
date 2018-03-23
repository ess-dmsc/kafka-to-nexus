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
/// @brief      Class that helps the StreamMaster to define its status.
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
  /// @brief      Determines if the streams and the report are closed.
  ///
  /// @return     True if has finished, False otherwise.
  ///
  bool hasFinished() const { return Value == 2; }
  //----------------------------------------------------------------------------
  /// @brief      Determines if the Master can safely remove and destroy the
  /// current StreamMaster.
  ///
  /// @return     True if removable, False otherwise.
  ///
  bool isRemovable() const { return Value == 4; }
  //----------------------------------------------------------------------------
  /// @brief      Determines if the StreamMaster is in a non-failure status.
  ///
  /// @return     True if ok, False otherwise.
  ///
  bool isOK() const { return Value > 0; }
  //----------------------------------------------------------------------------
  /// @brief      Compare two different StreamMasterStatus
  ///
  /// @param[in]  Other  The other.
  ///
  /// @return     True if the status is the same, False otherwise.
  ///
  bool operator==(const StreamMasterError &Other) {
    return Value == Other.Value;
  }

private:
  int Value{0};
};

//------------------------------------------------------------------------------
/// @brief      Class that helps the Streamer to define its status.
///
class StreamerError {
  friend const std::string Err2Str(const StreamerError &);

public:
  static StreamerError OK();
  static StreamerError WRITING();
  static StreamerError HAS_FINISHED();
  static StreamerError NOT_INITIALIZED();
  static StreamerError CONFIGURATION_ERROR();
  static StreamerError TOPIC_PARTITION_ERROR();
  static StreamerError UNKNOWN_ERROR();

  //----------------------------------------------------------------------------
  /// @brief      Determines if the connection to the Kafka broker is
  /// successfull.
  ///
  /// @return     True if the connection is established, False otherwise
  ///
  bool connectionOK() const { return Value >= 0; }
  //----------------------------------------------------------------------------
  /// @brief      Determines if the Streamer has finished its job.
  ///
  /// @return     True if the stop time has been reached or if the stop command
  /// has been received, False otherwise.
  ///
  bool hasFinished() const { return Value == 0; }
  //----------------------------------------------------------------------------
  /// @brief      Compare two different StreamerStatus
  ///
  /// @param[in]  Other  The other
  ///
  /// @return     True if the status is the same, False otherwise.
  ///
  bool operator==(const StreamerError &Other) { return Value == Other.Value; }

private:
  int Value{-1000};
};

//------------------------------------------------------------------------------
/// @brief      Converts a StreamerError status into a human readable string.
///
/// @param[in]  Error  The error status.
///
/// @return     The string that briefly describe the status.
///
const std::string Err2Str(const StreamerError &Error);

//------------------------------------------------------------------------------
/// @brief      Converts a StreamMasterError status into a human readable
/// string.
///
/// @param[in]  Error  The error status.
///
/// @return     The string that briefly describe the status.
///
const std::string Err2Str(const StreamMasterError &Error);

} // namespace Status
} // namespace FileWriter
