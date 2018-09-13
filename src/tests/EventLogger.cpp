#include "../EventLogger.h"

#include <gtest/gtest.h>

class StringProducer {
public:
  void produce(unsigned char *MsgData, size_t MsgSize,
               bool PrintError = false) {
    Message = std::string(reinterpret_cast<char *>(MsgData));
  }
  std::string Message;
};

TEST(EventLogger, produceSimpleLog) {
  std::shared_ptr<StringProducer> Producer{std::make_shared<StringProducer>()};
  logEvent(Producer, FileWriter::StatusCode::Start, "service-id-00",
           "job-id-01", "generic-start-message");
  nlohmann::json Produced = nlohmann::json::parse(Producer->Message);

  EXPECT_EQ(Produced["code"], "START");
  EXPECT_EQ(Produced["service_id"], "service-id-00");
  EXPECT_EQ(Produced["job_id"], "job-id-01");
  EXPECT_EQ(Produced["message"], "generic-start-message");
}

TEST(EventLogger, produceMultipleLogs) {
  std::shared_ptr<StringProducer> Producer{std::make_shared<StringProducer>()};
  logEvent(Producer, FileWriter::StatusCode::Start, "service-id-00",
           "job-id-01", "generic-start-message");
  std::string First = Producer->Message;

  logEvent(Producer, FileWriter::StatusCode::Start, "service-id-01",
           "job-id-02", "next-start-message");
  std::string Second = Producer->Message;

  nlohmann::json Produced = nlohmann::json::parse(Second);

  EXPECT_EQ(Produced["code"], "START");
  EXPECT_EQ(Produced["service_id"], "service-id-01");
  EXPECT_EQ(Produced["job_id"], "job-id-02");
  EXPECT_EQ(Produced["message"], "next-start-message");
}

TEST(EventLogger, eventCodes) {
  std::shared_ptr<StringProducer> Producer{std::make_shared<StringProducer>()};
  nlohmann::json Produced;

  logEvent(Producer, FileWriter::StatusCode::Start, "", "", "");
  Produced = nlohmann::json::parse(Producer->Message);
  EXPECT_EQ(Produced["code"], "START");

  logEvent(Producer, FileWriter::StatusCode::Close, "", "", "");
  Produced = nlohmann::json::parse(Producer->Message);
  EXPECT_EQ(Produced["code"], "CLOSE");

  logEvent(Producer, FileWriter::StatusCode::Error, "", "", "");
  Produced = nlohmann::json::parse(Producer->Message);
  EXPECT_EQ(Produced["code"], "ERROR");

  logEvent(Producer, FileWriter::StatusCode::Fail, "", "", "");
  Produced = nlohmann::json::parse(Producer->Message);
  EXPECT_EQ(Produced["code"], "FAIL");
}

TEST(EventLogger, logUsingClass) {
  std::shared_ptr<FileWriter::EventLogger> LoggerObject{
      std::make_unique<FileWriter::EventLogger>()};
  std::shared_ptr<StringProducer> Producer{std::make_shared<StringProducer>()};

  LoggerObject->create(Producer, "service-id-02", "job-id-03");
  LoggerObject->log(FileWriter::StatusCode::Start, "message-using-object");
  nlohmann::json Produced = nlohmann::json::parse(Producer->Message);

  EXPECT_EQ(Produced["code"], "START");
  EXPECT_EQ(Produced["service_id"], "service-id-02");
  EXPECT_EQ(Produced["job_id"], "job-id-03");
  EXPECT_EQ(Produced["message"], "message-using-object");
}

TEST(EventLogger, serviceAndJobIdOutOfScope) {
  std::shared_ptr<FileWriter::EventLogger> LoggerObject{
      std::make_unique<FileWriter::EventLogger>()};
  std::shared_ptr<StringProducer> Producer{std::make_shared<StringProducer>()};

  {
    std::string TmpServiceId{"service-id-03"};
    std::string TmpJobId{"job-id-04"};
    LoggerObject->create(Producer, TmpServiceId, TmpJobId);
  }
  LoggerObject->log(FileWriter::StatusCode::Start, "message-using-object");

  nlohmann::json Produced = nlohmann::json::parse(Producer->Message);

  EXPECT_EQ(Produced["code"], "START");
  EXPECT_EQ(Produced["service_id"], "service-id-03");
  EXPECT_EQ(Produced["job_id"], "job-id-04");
  EXPECT_EQ(Produced["message"], "message-using-object");
}
