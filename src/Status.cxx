#include <cmath>

#include "Status.h"
#include "logger.h"

/// Return the average given the sum of the elements and their number
/// \param sum the sum of the elements
/// \param N number of elements
double average(const double &Sum, const double &N) { return Sum / N; }

/// Return the unbiased standard deviation computed as \f$\sigma =
/// \sqrt{\frac{\langle x^2 \rangle - \langle x \rangle^2}{N(N-1)}}\f$
double standardDeviation(const double &Sum, const double &SumSquared,
                         const double &N) {
  double Variance = (SumSquared - (Sum * Sum) / N) / (N - 1);
  if (Variance > 0) { // can be caused by numerical instabilities
    return std::sqrt(Variance);
  } else {
    return 0.0;
  }
}

std::pair<double, double>
FileWriter::Status::messageSize(const FileWriter::Status::MessageInfo &Value) {
  if (Value.getMessages().first == 0) { // nan causes failure in JSON
    return std::pair<double, double>{};
  }
  std::pair<double, double> result(
      average(Value.getMbytes().first, Value.getMessages().first),
      standardDeviation(Value.getMbytes().first, Value.getMbytes().second,
                        Value.getMessages().first));
  return result;
}

double FileWriter::Status::messageFrequency(
    const FileWriter::Status::MessageInfo &Value,
    const std::chrono::milliseconds &TimeDifference) {
  if (TimeDifference.count() < 1e-10) {
    return 0.0;
  }
  return 1e3 * average(Value.getMessages().first, TimeDifference.count());
}

double FileWriter::Status::messageThroughput(
    const FileWriter::Status::MessageInfo &Value,
    const std::chrono::milliseconds &TimeDifference) {
  if (TimeDifference.count() < 1e-10) {
    return 0.0;
  }
  return 1e3 * average(Value.getMbytes().first, TimeDifference.count());
}

void FileWriter::Status::MessageInfo::message(const double &MessageSize) {
  std::lock_guard<std::mutex> Lock(Mutex);
  double Size = MessageSize * 1e-6;
  Mbytes += Size;
  MbytesSquare += Size * Size;
  Messages++;
  MessagesSquare++;
}

void FileWriter::Status::MessageInfo::error() {
  std::lock_guard<std::mutex> lock(Mutex);
  Errors++;
}

void FileWriter::Status::MessageInfo::reset() {
  Mbytes = MbytesSquare = 0.0;
  Messages = MessagesSquare = 0.0;
  Errors = 0.0;
}

std::pair<double, double> FileWriter::Status::MessageInfo::getMbytes() const {
  return std::pair<double, double>{Mbytes, MbytesSquare};
}

std::pair<double, double> FileWriter::Status::MessageInfo::getMessages() const {
  return std::pair<double, double>{Messages, MessagesSquare};
}

double FileWriter::Status::MessageInfo::getErrors() const { return Errors; }

std::pair<double, double> &operator+=(std::pair<double, double> &First,
                                      const std::pair<double, double> &Second) {
  First.first += Second.first;
  First.second += Second.second;
  return First;
}

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
FileWriter::Status::StreamMasterInfo::getTimeToNextMessage() {
  return MillisecondsToNextMessage;
}
const std::chrono::milliseconds
FileWriter::Status::StreamMasterInfo::runTime() {
  auto result = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now() - StartTime);
  return result;
}

std::pair<double, double>
FileWriter::Status::StreamMasterInfo::getMbytes() const {
  return Mbytes;
}

std::pair<double, double>
FileWriter::Status::StreamMasterInfo::getMessages() const {
  return Messages;
}

double FileWriter::Status::StreamMasterInfo::getErrors() const {
  return Errors;
}
