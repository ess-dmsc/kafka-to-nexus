#include "../EventLogger.h"

#include <gtest/gtest.h>

class StringProducer {
public:
  void produce(unsigned char *MsgData, size_t MsgSize) {
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
