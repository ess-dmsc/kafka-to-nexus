#include "KafkaGraylogInterface.h"
#include "KafkaW/KafkaW.h"
#include <ciso646>
#include <cstring>
#include <nlohmann/json.hpp>

std::string logMsgToJSON(const Log::LogMessage &Message) {
  using std::chrono::duration_cast;
  using std::chrono::milliseconds;

  nlohmann::json JsonObject;
  JsonObject["short_message"] = Message.MessageString;
  JsonObject["version"] = "1.1";
  JsonObject["level"] = int(Message.SeverityLevel);
  JsonObject["host"] = Message.Host;
  JsonObject["timestamp"] =
      static_cast<double>(
          duration_cast<milliseconds>(Message.Timestamp.time_since_epoch())
              .count()) /
      1000;
  JsonObject["_process_id"] = Message.ProcessId;
  JsonObject["_process"] = Message.ProcessName;
  JsonObject["_thread_id"] = Message.ThreadId;
  for (auto &field : Message.AdditionalFields) {
    if (Log::AdditionalField::Type::typeStr == field.second.FieldType) {
      JsonObject["_" + field.first] = field.second.strVal;
    } else if (Log::AdditionalField::Type::typeDbl == field.second.FieldType) {
      JsonObject["_" + field.first] = field.second.dblVal;
    } else if (Log::AdditionalField::Type::typeInt == field.second.FieldType) {
      JsonObject["_" + field.first] = field.second.intVal;
    }
  }
  return JsonObject.dump();
}

KafkaGraylogInterface::KafkaGraylogInterface(std::string BrokerAddress,
                                             std::string Topic,
                                             const size_t MaxQueueLength)
    : Log::BaseLogHandler(MaxQueueLength), Broker(std::move(BrokerAddress)),
      TopicName(std::move(Topic)),
      KafkaGraylogThread(&KafkaGraylogInterface::threadFunction, this) {}

KafkaGraylogInterface::KafkaGraylogInterface() { exitThread(); }

void KafkaGraylogInterface::exitThread() {
  Log::LogMessage ExitMsg;
  ExitMsg.MessageString = "exit";
  ExitMsg.ProcessId = -1;
  Log::BaseLogHandler::addMessage(ExitMsg);
  if (KafkaGraylogThread.joinable()) {
    KafkaGraylogThread.join();
  }
}

void KafkaGraylogInterface::threadFunction() {
  Log::LogMessage TmpMsg;
  KafkaW::BrokerSettings BrokerSettings;
  BrokerSettings.Address = Broker;

  auto Producer = std::make_shared<KafkaW::Producer>(BrokerSettings);
  auto Topic = std::make_shared<KafkaW::Producer::Topic>(Producer, TopicName);
  Topic->enableCopy();

  int MessageTimeoutMS = 50;
  while (true) {
    bool GotMessage = MessageQueue.time_out_pop(TmpMsg, MessageTimeoutMS);
    if (GotMessage) {
      if (std::string("exit") == TmpMsg.MessageString and
          -1 == TmpMsg.ProcessId) {
        break;
      }
      auto GraylogMessage = logMsgToJSON(TmpMsg);
      Topic->produce((KafkaW::uchar *)GraylogMessage.data(),
                     GraylogMessage.size());
    }
    Producer->poll();
  }
}
