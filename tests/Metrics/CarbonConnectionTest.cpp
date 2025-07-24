#include "CarbonTestServer.h"
#include "Metrics/CarbonInterface.h"
#include "Metrics/CarbonSink.h"
#include "Metrics/Metric.h"
#include "Metrics/Registrar.h"
#include "Metrics/Reporter.h"
#include <chrono>
#include <gtest/gtest.h>
#include <regex>

using namespace std::chrono_literals;

class DISABLED_MetricsCarbonConnectionTest : public ::testing::Test {
public:
  void SetUp() override {
    UsedPort += 1;
    CarbonServer = std::make_unique<CarbonTestServer>(UsedPort);
  }

  void TearDown() override { CarbonServer.reset(); }
  std::uint16_t UsedPort{6587};
  std::unique_ptr<CarbonTestServer> CarbonServer;
  std::chrono::system_clock::duration SleepTime{100ms};
};

TEST_F(DISABLED_MetricsCarbonConnectionTest, UnknownHost) {
  Metrics::Carbon::Connection con("no_host", UsedPort);
  std::this_thread::sleep_for(SleepTime);
  ASSERT_EQ(CarbonServer->GetNrOfConnections(), 0);
  ASSERT_EQ(CarbonServer->GetLatestMessage().size(), 0ul);
  ASSERT_TRUE(!CarbonServer->GetLastSocketError());
  EXPECT_NE(con.getConnectionStatus(), Metrics::Carbon::Status::SEND_LOOP);
}

TEST_F(DISABLED_MetricsCarbonConnectionTest, Connection) {
  ASSERT_EQ(0l, CarbonServer->GetNrOfConnections());
  ASSERT_EQ(0ul, CarbonServer->GetLatestMessage().size());
  ASSERT_TRUE(!CarbonServer->GetLastSocketError());
  {
    Metrics::Carbon::Connection con("localhost", UsedPort);
    std::this_thread::sleep_for(SleepTime);
    ASSERT_EQ(1, CarbonServer->GetNrOfConnections());
    ASSERT_EQ(CarbonServer->GetLatestMessage().size(), 0ul);
    ASSERT_TRUE(!CarbonServer->GetLastSocketError());
    ASSERT_EQ(Metrics::Carbon::Status::SEND_LOOP, con.getConnectionStatus())
        << "Connection status returned " << int(con.getConnectionStatus());
  }
  std::this_thread::sleep_for(SleepTime);
  ASSERT_EQ(0l, CarbonServer->GetNrOfConnections());
  ASSERT_EQ(0ul, CarbonServer->GetLatestMessage().size());
  auto SocketError = CarbonServer->GetLastSocketError();
#ifdef WIN32
  ASSERT_TRUE(SocketError == asio::error::connection_reset);
#else
  ASSERT_TRUE(SocketError == asio::error::misc_errors::eof);
#endif
}

TEST_F(DISABLED_MetricsCarbonConnectionTest, IPv6Connection) {
  ASSERT_EQ(0, CarbonServer->GetNrOfConnections());
  ASSERT_EQ(0ul, CarbonServer->GetLatestMessage().size());
  ASSERT_TRUE(!CarbonServer->GetLastSocketError());
  {
    Metrics::Carbon::Connection con("::1", UsedPort);
    std::this_thread::sleep_for(SleepTime);
    ASSERT_EQ(1, CarbonServer->GetNrOfConnections());
    ASSERT_EQ(CarbonServer->GetLatestMessage().size(), 0ul);
    ASSERT_TRUE(!CarbonServer->GetLastSocketError());
    ASSERT_EQ(Metrics::Carbon::Status::SEND_LOOP, con.getConnectionStatus())
        << "Connection status returned " << int(con.getConnectionStatus());
  }
  std::this_thread::sleep_for(SleepTime);
  ASSERT_EQ(0, CarbonServer->GetNrOfConnections());
  ASSERT_EQ(0ul, CarbonServer->GetLatestMessage().size());
  auto SocketError = CarbonServer->GetLastSocketError();
#ifdef WIN32
  ASSERT_TRUE(SocketError == asio::error::connection_reset);
#else
  ASSERT_TRUE(SocketError == asio::error::misc_errors::eof);
#endif
}

TEST_F(DISABLED_MetricsCarbonConnectionTest, WrongPort) {
  Metrics::Carbon::Connection con("localhost", UsedPort + 1);
  std::this_thread::sleep_for(SleepTime);
  ASSERT_EQ(CarbonServer->GetNrOfConnections(), 0);
  ASSERT_EQ(CarbonServer->GetLatestMessage().size(), 0ul);
  ASSERT_TRUE(!CarbonServer->GetLastSocketError());
  EXPECT_NE(con.getConnectionStatus(), Metrics::Carbon::Status::SEND_LOOP);
}

TEST_F(DISABLED_MetricsCarbonConnectionTest, CloseConnection) {
  {
    Metrics::Carbon::Connection con("localhost", UsedPort);
    std::this_thread::sleep_for(SleepTime);
    ASSERT_EQ(Metrics::Carbon::Status::SEND_LOOP, con.getConnectionStatus());
    ASSERT_EQ(1, CarbonServer->GetNrOfConnections());
    CarbonServer->CloseAllConnections();
    std::this_thread::sleep_for(SleepTime * 2);
    EXPECT_EQ(Metrics::Carbon::Status::SEND_LOOP, con.getConnectionStatus());
    EXPECT_EQ(1, CarbonServer->GetNrOfConnections())
        << "Failed to reconnect after connection was closed remotely.";
  }
  std::this_thread::sleep_for(SleepTime);
  EXPECT_EQ(0, CarbonServer->GetNrOfConnections());
}

TEST_F(DISABLED_MetricsCarbonConnectionTest, MessageTransmission) {
  {
    std::string testString("This is a test string!");
    Metrics::Carbon::Connection con("localhost", UsedPort);
    con.sendMessage(testString);
    std::this_thread::sleep_for(SleepTime);
    ASSERT_TRUE(!CarbonServer->GetLastSocketError());
    ASSERT_EQ(testString.size() + 1ul, CarbonServer->GetReceivedBytes());
    ASSERT_EQ(testString, CarbonServer->GetLatestMessage());
    ASSERT_EQ(1, CarbonServer->GetNrOfConnections());
  }
}

TEST_F(DISABLED_MetricsCarbonConnectionTest, LargeMessageTransmission) {
  {
    std::string RepeatedString("This is a test string!");
    std::string TargetString;
    for (size_t i = 0; i < 30000; i++) {
      TargetString += RepeatedString;
    }
    Metrics::Carbon::Connection con("localhost", UsedPort);
    con.sendMessage(TargetString);
    std::this_thread::sleep_for(SleepTime);
    ASSERT_TRUE(!CarbonServer->GetLastSocketError());
    ASSERT_EQ(TargetString.size() + 1, CarbonServer->GetReceivedBytes());
    ASSERT_EQ(TargetString, CarbonServer->GetLatestMessage());
    ASSERT_EQ(1, CarbonServer->GetNrOfConnections());
  }
}

TEST_F(DISABLED_MetricsCarbonConnectionTest, MultipleMessages) {
  std::vector<std::string> lines = {"This is a test.", R"(!"#€%&/()=?*^_-.,:;)",
                                    "Another line bites the dust."};
  {
    Metrics::Carbon::Connection con("localhost", UsedPort);
    std::this_thread::sleep_for(SleepTime);
    size_t TotalBytes = 0;
    for (auto ln : lines) {
      TotalBytes += (ln.size() + 1);
      con.sendMessage(ln);
    }
    std::this_thread::sleep_for(SleepTime);
    ASSERT_TRUE(!CarbonServer->GetLastSocketError());
    ASSERT_EQ(lines[lines.size() - 1], CarbonServer->GetLatestMessage());
    ASSERT_EQ(TotalBytes, CarbonServer->GetReceivedBytes());
    ASSERT_EQ(1, CarbonServer->GetNrOfConnections());
  }
}

TEST_F(DISABLED_MetricsCarbonConnectionTest, DISABLED_MultipleCloseConnection) {
  {
    Metrics::Carbon::Connection con("localhost", UsedPort);
    std::string SomeTestMessage(
        "Hello, this is some test message with the number ");
    int NrOfMessages = 100;
    for (int i = 0; i < NrOfMessages; ++i) {
      con.sendMessage(SomeTestMessage + std::to_string(i));
      CarbonServer->CloseAllConnections();
      std::this_thread::sleep_for(30ms);
    }
    std::this_thread::sleep_for(1000ms);
    EXPECT_EQ(CarbonServer->GetNrOfMessages(), NrOfMessages);
  }
}

TEST_F(DISABLED_MetricsCarbonConnectionTest, SendUpdate) {
  auto TestName = std::string("SomeLongWindedName");
  Metrics::CounterType Ctr{112233};
  auto Description = "A long description of a metric.";
  auto TestSink = std::unique_ptr<Metrics::Sink>(
      new Metrics::CarbonSink("localhost", UsedPort));
  auto TestReporter =
      std::make_shared<Metrics::Reporter>(std::move(TestSink), 10ms);
  std::vector<std::shared_ptr<Metrics::Reporter>> TestReporters{TestReporter};
  auto TestRegistrar =
      std::make_shared<Metrics::Registrar>("Test", TestReporters);
  auto TestMetric = std::make_shared<Metrics::Metric>(TestName, Description,
                                                      Metrics::Severity::ERROR);

  TestRegistrar->registerMetric(TestMetric, {Metrics::LogTo::CARBON});
  std::this_thread::sleep_for(200ms);
  ASSERT_GE(CarbonServer->GetNrOfMessages(), 0);
  auto LastCarbonString = CarbonServer->GetLatestMessage();
  std::regex Regex(TestName + " " + std::to_string(Ctr) + " \\d+\n");
  std::smatch Matches;
  std::regex_match(LastCarbonString, Matches, Regex);
  EXPECT_TRUE(!Matches.empty());
}
