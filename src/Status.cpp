#include <cmath>

#include "Status.h"
#include "logger.h"

/// \brief Return the average given the sum of the elements and their number
/// \param sum the sum of the elements
/// \param N number of elements
double average(double Sum, double N) { return Sum / N; }

/// Return the unbiased standard deviation computed as \f$\sigma =
/// \sqrt{\frac{\langle x^2 \rangle - \langle x \rangle^2}{N(N-1)}}\f$
double standardDeviation(double Sum, double SumSquared, double N) {
  // Avoid divide-by-zero error due to too few messages.
  if (N <= 1.0) {
    return 0.0;
  }

  double Variance = (SumSquared - (Sum * Sum) / N) / (N - 1);
  if (Variance > 0) { // can be caused by numerical instabilities
    return std::sqrt(Variance);
  }
  return 0.0;
}

std::pair<double, double> FileWriter::Status::MessageInfo::messageSize() const {
  if (Mbytes == 0) { // nan causes failure in JSON
    return std::pair<double, double>{};
  }
  std::pair<double, double> result(
      average(Mbytes, Messages),
      standardDeviation(Mbytes, MbytesSquare, Messages));
  return result;
}

void FileWriter::Status::MessageInfo::newMessage(const double &MessageBytes) {
  std::lock_guard<std::mutex> Lock(Mutex);
  double Size = MessageBytes * 1e-6;
  Mbytes += Size;
  MbytesSquare += Size * Size;
  Messages++;
}

void FileWriter::Status::MessageInfo::error() {
  std::lock_guard<std::mutex> lock(Mutex);
  Errors++;
}

void FileWriter::Status::MessageInfo::reset() {
  Mbytes = MbytesSquare = 0.0;
  Messages = 0;
  Errors = 0;
}

double FileWriter::Status::MessageInfo::getMbytes() const { return Mbytes; }

uint64_t FileWriter::Status::MessageInfo::getMessages() const {
  return Messages;
}

uint64_t FileWriter::Status::MessageInfo::getErrors() const { return Errors; }

void FileWriter::Status::StreamMasterInfo::add(
    FileWriter::Status::MessageInfo &Info) {
  Mbytes += Info.getMbytes();
  Messages += Info.getMessages();
  Errors += Info.getErrors();
  Info.reset();
}

void FileWriter::Status::StreamMasterInfo::setTimeToNextMessage(
    const std::chrono::milliseconds &ToNextMessage) {
  MillisecondsToNextMessage = ToNextMessage;
}
const std::chrono::milliseconds
FileWriter::Status::StreamMasterInfo::getTimeToNextMessage() const {
  return MillisecondsToNextMessage;
}
const std::chrono::milliseconds
FileWriter::Status::StreamMasterInfo::runTime() const {
  auto result = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now() - StartTime);
  return result;
}

double FileWriter::Status::StreamMasterInfo::getMbytes() const {
  return Mbytes;
}

uint64_t FileWriter::Status::StreamMasterInfo::getMessages() const {
  return Messages;
}

uint64_t FileWriter::Status::StreamMasterInfo::getErrors() const {
  return Errors;
}
