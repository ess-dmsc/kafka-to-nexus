#include <cstdio>

#include "Status.h"
#include "StatusWriter.h"

#include "rapidjson/filewritestream.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/writer.h"

namespace FileWriter {
namespace Status {

/// Serialize the information contained in a StreamMasterInfo object
/// into a JSON message stored as a rapidjson::Document.
/// \param Information the StreamMasterInfo object to be serialized
rapidjson::Document
JSONWriterBase::writeImplemented(StreamMasterInfo &Information) const {
  rapidjson::Document Document;
  auto &Allocator = Document.GetAllocator();
  Document.SetObject();

  milliseconds next_message_relative_eta_ms = Information.timeToNextMessage();
  { // message type
    Document.AddMember("type", "stream_master_status", Allocator);
    Document.AddMember("next_message_eta_ms",
                       next_message_relative_eta_ms.count(), Allocator);
  }
  { // stream master info
    rapidjson::Value StreamMasterInformation;
    StreamMasterInformation.SetObject();
    StreamMasterInformation.AddMember(
        "state", rapidjson::StringRef(Err2Str(Information.status()).c_str()),
        Allocator);
    StreamMasterInformation.AddMember(
        "messages", Information.getTotal().getMessages().first, Allocator);
    StreamMasterInformation.AddMember(
        "Mbytes", Information.getTotal().getMbytes().first, Allocator);
    StreamMasterInformation.AddMember(
        "errors", Information.getTotal().getErrors(), Allocator);
    StreamMasterInformation.AddMember("runtime", Information.runTime().count(),
                                      Allocator);
    Document.AddMember("stream_master", StreamMasterInformation, Allocator);
  }
  { // streamers info
    rapidjson::Value StreamerInformation;
    StreamerInformation.SetObject();
    for (auto &TopicName : Information.info()) {
      rapidjson::Value Key(TopicName.first.c_str(), Allocator);
      rapidjson::Value Value;
      Value.SetObject();
      Value.AddMember("status", primaryQuantities(TopicName.second, Allocator),
                      Allocator);
      Value.AddMember("statistics",
                      derivedQuantities(TopicName.second,
                                        next_message_relative_eta_ms,
                                        Allocator),
                      Allocator);
      StreamerInformation.AddMember(Key, Value, Allocator);
    }
    Document.AddMember("streamer", StreamerInformation, Allocator);
  }
  return Document;
}

/// Format the number received of messages, the received megabytes and the
/// number of errors in a rapidjson::Value object
/// \param Information a MessageInfo object containing information about the
/// messages consumed by the Streamer
/// \param Allocator the rapidjson::Allocator for the rapidjson::Document to be
/// written
template <class AllocatorType>
rapidjson::Value
JSONWriterBase::primaryQuantities(MessageInfo &Information,
                                  AllocatorType &Allocator) const {
  rapidjson::Value Value;
  Value.SetObject();
  Value.AddMember("messages", Information.getMessages().first, Allocator);
  Value.AddMember("Mbytes", Information.getMbytes().first, Allocator);
  Value.AddMember("errors", Information.getErrors(), Allocator);
  return Value;
}

/// Return the average and the standard value of the quantity as a
/// rapidjson::Value object
/// \param Quantity a MessageInfo::value_type object containing statistical
/// information about the quantity of interest
/// \param Allocator the rapidjson::Allocator for the rapidjson::Document to be
/// written
template <class AllocatorType>
rapidjson::Value createDerivedQuantity(MessageInfo::value_type &Quantity,
                                       AllocatorType &Allocator) {
  rapidjson::Value Result;
  Result.SetObject();
  Result.AddMember("average", Quantity.first, Allocator);
  Result.AddMember("stdandard_deviation", Quantity.second, Allocator);
  return Result;
}

/// Compute average message size, the message frequency and the Streamer
/// throughput and return them as a rapidjson::Value object
/// \param Information a MessageInfo object containing information about the
/// messages consumed by the Streamer
/// \param Duration
/// \param Allocator the rapidjson::Allocator for the rapidjson::Document to be
/// written
template <class AllocatorType>
rapidjson::Value
JSONWriterBase::derivedQuantities(MessageInfo &Info,
                                  const milliseconds &Duration,
                                  AllocatorType &Allocator) const {
  auto Size = messageSize(Info);
  auto Frequency = FileWriter::Status::messageFrequency(Info, Duration);
  auto Throughput = FileWriter::Status::messageThroughput(Info, Duration);

  rapidjson::Value Value;
  Value.SetObject();
  Value.AddMember("size", createDerivedQuantity(Size, Allocator), Allocator);
  Value.AddMember("frequency", createDerivedQuantity(Frequency, Allocator),
                  Allocator);
  Value.AddMember("throughput", createDerivedQuantity(Throughput, Allocator),
                  Allocator);

  return Value;
}

JSONStreamWriter::ReturnType
JSONStreamWriter::write(StreamMasterInfo &Information) const {
  auto Value = Base.writeImplemented(Information);
  rapidjson::StringBuffer Buffer;
  rapidjson::PrettyWriter<rapidjson::StringBuffer> Writer(Buffer);
  Writer.SetMaxDecimalPlaces(1);
  Value.Accept(Writer);
  std::string String(Buffer.GetString());
  return String;
}

JSONWriter::ReturnType JSONWriter::write(StreamMasterInfo &Information) const {
  return Base.writeImplemented(Information);
}

} // namespace Status

} // namespace FileWriter
