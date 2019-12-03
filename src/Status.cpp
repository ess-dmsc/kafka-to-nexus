// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include <cmath>

#include "Status.h"
#include "logger.h"

void FileWriter::Status::MessageInfo::incrementTotalMessageCount() {
  std::lock_guard<std::mutex> lock(Mutex);
  ++Messages;
}

void FileWriter::Status::MessageInfo::incrementProcessedCount(double MsgSize) {
  std::lock_guard<std::mutex> lock(Mutex);
  TotalBytesProcessed += MsgSize;
  ++ProcessedMessages;
}

void FileWriter::Status::MessageInfo::incrementWriteError() {
  std::lock_guard<std::mutex> lock(Mutex);
  ++WriteErrors;
}

void FileWriter::Status::MessageInfo::incrementValidationErrors() {
  std::lock_guard<std::mutex> lock(Mutex);
  ++ValidationErrors;
}

void FileWriter::Status::MessageInfo::resetStatistics() {
  std::lock_guard<std::mutex> Lock(Mutex);
  TotalBytesProcessed = 0;
  Messages = 0;
  WriteErrors = 0;
  ProcessedMessages = 0;
}

uint64_t FileWriter::Status::MessageInfo::getNumberMessages() const {
  std::lock_guard<std::mutex> Lock(Mutex);
  return Messages;
}

double FileWriter::Status::MessageInfo::getMbytes() const {
  std::lock_guard<std::mutex> Lock(Mutex);
  return TotalBytesProcessed * 1e-6;
}

uint64_t FileWriter::Status::MessageInfo::getNumberProcessedMessages() const {
  std::lock_guard<std::mutex> Lock(Mutex);
  return ProcessedMessages;
}

uint64_t FileWriter::Status::MessageInfo::getNumberWriteErrors() const {
  std::lock_guard<std::mutex> Lock(Mutex);
  return WriteErrors;
}

uint64_t FileWriter::Status::MessageInfo::getNumberValidationErrors() const {
  std::lock_guard<std::mutex> Lock(Mutex);
  return ValidationErrors;
}

void FileWriter::Status::StreamMasterInfo::add(
    FileWriter::Status::MessageInfo &Info) {
  Mbytes += Info.getMbytes();
  Messages += Info.getNumberMessages();
  SuccessfullyProcessedMessages += Info.getNumberProcessedMessages();
  Errors += Info.getNumberWriteErrors();
  ValidationErrors += Info.getNumberValidationErrors();
  Info.resetStatistics();
}

void FileWriter::Status::StreamMasterInfo::setTimeToNextMessage(
    const std::chrono::milliseconds &ToNextMessage) {
  MillisecondsToNextMessage = ToNextMessage;
}

std::chrono::milliseconds
FileWriter::Status::StreamMasterInfo::getTimeToNextMessage() const {
  return MillisecondsToNextMessage;
}

std::chrono::milliseconds
FileWriter::Status::StreamMasterInfo::runTime() const {
  auto result = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now() - StartTime);
  return result;
}

double FileWriter::Status::StreamMasterInfo::getMbytes() const {
  return Mbytes;
}

uint64_t FileWriter::Status::StreamMasterInfo::getNumberMessages() const {
  return Messages;
}

uint64_t
FileWriter::Status::StreamMasterInfo::getNumberProcessedMessages() const {
  return SuccessfullyProcessedMessages;
}

uint64_t
FileWriter::Status::StreamMasterInfo::getNumberValidationErrors() const {
  return ValidationErrors;
}

uint64_t FileWriter::Status::StreamMasterInfo::getNumberErrors() const {
  return Errors;
}
