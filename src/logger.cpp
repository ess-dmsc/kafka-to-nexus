#include "logger.h"
#include "KafkaGraylogInterface.h"
#include "KafkaW/KafkaW.h"
#include "json.h"
#include "uri.h"
#include <atomic>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <graylog_logger/ConsoleInterface.hpp>
#include <graylog_logger/FileInterface.hpp>
#include <graylog_logger/GraylogInterface.hpp>
#include <memory>
#include <string>
#include <thread>

std::string Formatter(const Log::LogMessage &Msg) {
  std::string FileName;
  std::int64_t LineNr = -1;
  std::string ServiceID;
  for (auto &CField : Msg.AdditionalFields) {
    if (CField.first == "file") {
      FileName = CField.second.strVal;
    } else if (CField.first == "line") {
      LineNr = CField.second.intVal;
    } else if (CField.first == "ServiceID") {
      ServiceID = CField.second.strVal;
    }
  }
  return fmt::format("{}:{} [{}] [ServiceID:{}]:  {}", FileName, LineNr,
                     int(Msg.SeverityLevel), ServiceID, Msg.MessageString);
}

void addGraylogInterface(std::string Address) {
  if (Address.empty()) {
    return;
  }
  unsigned short Port = 12201;
  auto ColonPos = Address.find(":");
  if (ColonPos != std::string::npos) {
    try {
      Port = std::stoi(Address.substr(ColonPos + 1, std::string::npos));
    } catch (std::invalid_argument) {
      LOG(Sev::Warning, "Unable to extract port number from the string \"{}\", "
                        "using the default value of {}.",
          Address, Port);
    } catch (std::out_of_range) {
      LOG(Sev::Warning, "The port number in the string \"{}\" is out of range.",
          Address);
    }
  }
  LOG(Sev::Info, "Enable graylog_logger on {}:{}", Address.substr(0, ColonPos),
      Port);
  Log::AddLogHandler(
      new Log::GraylogInterface(Address.substr(0, ColonPos), Port));
}

void setupLogging(Sev LoggingLevel, std::string ServiceID, std::string LogFile,
                  std::string GraylogAddress, std::string KafkaGraylogUri) {
  Log::SetMinimumSeverity(LoggingLevel);

  Log::RemoveAllHandlers();
  Log::AddField("ServiceID", ServiceID);
  auto CI = new Log::ConsoleInterface();
  CI->setMessageStringCreatorFunction(Formatter);
  Log::AddLogHandler(CI);

  if (not LogFile.empty()) {
    auto TempHandler = new Log::FileInterface(LogFile);
    TempHandler->setMessageStringCreatorFunction(Formatter);
    Log::AddLogHandler(TempHandler);
  }

  addGraylogInterface(GraylogAddress);

  if (not KafkaGraylogUri.empty()) {
    uri::URI TempUri(KafkaGraylogUri);
    Log::AddLogHandler(new KafkaGraylogInterface(TempUri.host, TempUri.topic));
  }
}
