#include <algorithm>
#include <atomic>
#include <chrono>
#include <gtest/gtest.h>
#include <stdexcept>
#include <thread>

#include "../Streamer.hpp"
#include <librdkafka/rdkafkacpp.h>

#include "../uri.h"
#include "consumer.hpp"
#include "producer.hpp"

const int max_recv_messages = 10;

using namespace FileWriter;

// ///////////////////
// ///////////////////
// std::function<ProcessMessageResult(void *, int)> silent = [](void *x, int) {
//   return ProcessMessageResult::OK();
// };

// std::function<ProcessMessageResult(void *, int)> verbose = [](void *x,
//                                                               int size) {
//   std::cout << "message: " << x << "\t" << size << std::endl;
//   return ProcessMessageResult::OK();
// };
// std::function<ProcessMessageResult(void *, int, int *)> sum =
//     [](void *x, int size, int *data_size) {
//       (*data_size) += size;
//       return ProcessMessageResult::OK();
//     };

// std::pair<std::string, int64_t> dummy_message_parser(std::string &&msg) {
//   auto position = msg.find("-", 0);
//   int64_t timestamp;
//   if (position != std::string::npos) {
//     std::stringstream(msg.substr(position + 1)) >> timestamp;
//     return std::pair<std::string, int64_t>(msg.substr(0, position), timestamp);
//   }
//   // what to return??  make it an error.
//   return std::pair<std::string, int64_t>("", -1);
// }
// std::function<TimeDifferenceFromMessage_DT(void *, int)> time_difference =
//     [](void *x, int size) {
//       auto parsed_text = dummy_message_parser(std::string((char *)x));
//       return TimeDifferenceFromMessage_DT(parsed_text.first,
//                                           parsed_text.second);
//     };

// TEST(Streamer, MissingTopicFailure) {
//   ASSERT_THROW(Streamer(std::string("data_server:1234"), std::string("")),
//                std::runtime_error);
// }

// TEST(Streamer, ConstructionSuccess) {
//   ASSERT_NO_THROW(Streamer(Producer::broker, Producer::topic));
// }

// TEST(Streamer, NoReceive) {
//   Streamer s(Producer::broker, "dummy_topic");
//   using namespace std::placeholders;
//   int data_size = 0, counter = 0;
//   std::function<ProcessMessageResult(void *, int)> f1 =
//       std::bind(sum, _1, _2, &data_size);
//   ProcessMessageResult status = ProcessMessageResult::OK();

//   do {
//     EXPECT_TRUE(status.is_OK());
//     status = s.write(f1);
//     ++counter;
//   } while (status.is_OK() && (counter < max_recv_messages));

//   EXPECT_EQ(data_size, 0);
// }

// TEST_F(Producer, Receive) {
//   using namespace std::placeholders;

//   Streamer s(Producer::broker, Producer::topic);
//   int data_size = 0, counter = 0;
//   std::function<ProcessMessageResult(void *, int)> f1 =
//       std::bind(sum, _1, _2, &data_size);

//   start();

//   ProcessMessageResult status = ProcessMessageResult::OK();
//   status = s.write(silent);
//   ASSERT_TRUE(status.is_OK());

//   do {
//     EXPECT_TRUE(status.is_OK());
//     status = s.write(f1);
//     ++counter;
//   } while (status.is_OK() && (counter < max_recv_messages));
//   EXPECT_GT(data_size, 0);

//   stop();
// }

// TEST_F(Producer, Reconnect) {
//   using namespace std::placeholders;

//   int data_size = 0, counter = 0;
//   std::function<ProcessMessageResult(void *, int)> f1 =
//       std::bind(sum, _1, _2, &data_size);

//   start();

//   Streamer s(Producer::broker, "dummy_topic");
//   ProcessMessageResult status = ProcessMessageResult::OK();
//   do {
//     EXPECT_TRUE(status.is_OK());
//     status = s.write(f1);
//     ++counter;
//   } while (status.is_OK() && (counter < max_recv_messages));
//   EXPECT_FALSE(data_size > 0);
//   EXPECT_EQ(s.disconnect().value(), 0);

//   data_size = 0;
//   counter = 0;
//   EXPECT_EQ(s.connect(Producer::broker, Producer::topic), 0);
//   status = ProcessMessageResult::OK();
//   do {
//     EXPECT_TRUE(status.is_OK());
//     status = s.write(f1);
//     ++counter;
//   } while (status.is_OK() && (counter < max_recv_messages));
//   EXPECT_TRUE(data_size > 0);
//   stop();
// }

// TEST_F(Producer, JumpBack) {
//   Streamer s(broker, topic);
//   ProcessMessageResult status = ProcessMessageResult::OK();
//   int counter = 0;
//   start();
//   do {
//     EXPECT_TRUE(status.is_OK());
//     status = s.write(silent);
//     ++counter;
//   } while (status.is_OK() && (counter < max_recv_messages));

//   std::cout << "\n\nhello\n" << std::endl;
//   DemuxTopic demux(Producer::topic);
//   stop();
// }

TEST_F(Consumer, setup) {
  timestamp = 1511278317590;
  create_config();
  create_consumer();
  create_metadata();
  create_topic_partition();
  offsets_for_times();
  assign_topic_partition();
  while (1) {
    consume();
  }
}

TEST_F(Producer, setup) {}

TEST_F(Producer, produce_message) {
  produce("test",0);
}

int main(int argc, char **argv) {

  ::testing::InitGoogleTest(&argc, argv);

  for (int i = 1; i < argc; ++i) {
    if (std::string{argv[i]} == "--broker" || std::string{argv[i]} == "-b") {
      uri::URI u(argv[i + 1]);
      Producer::broker = u.host_port;
      Consumer::broker = u.host_port;
      Consumer::topic = u.topic;
    }
    if (std::string{argv[i]} == "--help" || std::string{argv[i]} == "-h") {
      std::cout << "\nOptions: "
                << "\n"
                << "\t--broker //<host>:<port>/<topic>\n";
    }
  }

  return RUN_ALL_TESTS();
}
