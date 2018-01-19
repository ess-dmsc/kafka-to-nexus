#include <cmath>

#include <rapidjson/document.h>

#include "Status.h"

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

const std::pair<double, double>
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

const std::pair<double, double> FileWriter::Status::messageFrequency(
    const FileWriter::Status::MessageInfo &Value,
    const milliseconds &TimeDifference) {
  if (TimeDifference.count() < 1e-10) {
    return std::pair<double, double>({0, 0});
  }
  std::pair<double, double> result(
      1e3 * average(Value.getMessages().first, TimeDifference.count()),
      1e3 * standardDeviation(Value.getMessages().first,
                              Value.getMessages().second,
                              TimeDifference.count()));
  return result;
}

const std::pair<double, double> FileWriter::Status::messageThroughput(
    const FileWriter::Status::MessageInfo &Value,
    const milliseconds &TimeDifference) {
  if (TimeDifference.count() < 1e-10) {
    return std::pair<double, double>({0, 0});
  }
  std::pair<double, double> result(
      1e3 * average(Value.getMbytes().first, TimeDifference.count()),
      1e3 * standardDeviation(Value.getMbytes().first, Value.getMbytes().second,
                              TimeDifference.count()));
  return result;
}

/// Implement atomic_add for a generic type. The type must implement operator +
/// and =
template <class T> void atomic_add(std::atomic<T> &Value, const T &Other) {
  T Current = Value.load();
  while (!Value.compare_exchange_weak(Current, Current + Other)) {
    Current = Value.load();
  }
}

const FileWriter::Status::MessageInfo &FileWriter::Status::MessageInfo::
operator=(const FileWriter::Status::MessageInfo &Other) {
  Mbytes.store(Other.Mbytes.load());
  MbytesSquare.store(Other.MbytesSquare.load());
  Messages.store(Other.Messages.load());
  MessagesSquare.store(Other.MessagesSquare.load());
  Errors.store(Other.Errors.load());
  return *this;
}

const FileWriter::Status::MessageInfo &FileWriter::Status::MessageInfo::
operator+=(const FileWriter::Status::MessageInfo &Other) {
  std::lock_guard<std::mutex> Lock(Mutex);
  atomic_add(Mbytes, Other.Mbytes.load());
  atomic_add(MbytesSquare, Other.MbytesSquare.load());
  atomic_add(Messages, Other.Messages.load());
  atomic_add(MessagesSquare, Other.MessagesSquare.load());
  atomic_add(Errors, Other.Errors.load());
  return *this;
}

const FileWriter::Status::MessageInfo &
FileWriter::Status::MessageInfo::message(const double &MessageSize) {
  std::lock_guard<std::mutex> Lock(Mutex);
  double Size = MessageSize * 1e-6;
  atomic_add(Mbytes, Size);
  atomic_add(MbytesSquare, Size * Size);
  atomic_add(Messages, 1.0);
  atomic_add(MessagesSquare, 1.0);
  return *this;
}

const FileWriter::Status::MessageInfo &
FileWriter::Status::MessageInfo::error() {
  std::lock_guard<std::mutex> lock(Mutex);
  atomic_add(Errors, 1.0);
  return *this;
}

void FileWriter::Status::MessageInfo::reset() {
  Mbytes = MbytesSquare = 0.0;
  Messages = MessagesSquare = 0.0;
  Errors = 0.0;
}

std::pair<double, double> FileWriter::Status::MessageInfo::getMbytes() const {
  return std::pair<double, double>{Mbytes.load(), MbytesSquare.load()};
}

std::pair<double, double> FileWriter::Status::MessageInfo::getMessages() const {
  return std::pair<double, double>{Messages.load(), MessagesSquare.load()};
}

double FileWriter::Status::MessageInfo::getErrors() const {
  return Errors.load();
}

////////////////////////
// StreamMasterInfo
void FileWriter::Status::StreamMasterInfo::add(
    const std::string &topic, FileWriter::Status::MessageInfo &info) {
  // The lock prevents Streamer::write from update info while
  // collecting these stats, or this function from reset while/after
  // Streamer::write updates. Pairs with MessageInfo::message(error) lock
  if (StreamsInfo.find(topic) != StreamsInfo.end()) {
    //    std::lock_guard<std::mutex> lock(info.getMutex());
    StreamsInfo[topic] = info;
    Total += info;
    info.reset();
  } else {
    //    std::lock_guard<std::mutex> lock(info.getMutex());
    StreamsInfo[topic] += info;
    Total += info;
    info.reset();
  }
}

void FileWriter::Status::StreamMasterInfo::setTimeToNextMessage(
    const milliseconds &ToNextMessage) {
  NextMessageRelativeEta = ToNextMessage;
}
const milliseconds
FileWriter::Status::StreamMasterInfo::getTimeToNextMessage() {
  return NextMessageRelativeEta;
}
const milliseconds FileWriter::Status::StreamMasterInfo::runTime() {
  auto result = std::chrono::duration_cast<milliseconds>(
      std::chrono::system_clock::now() - StartTime);
  return result;
}
