#include <atomic>
#include <chrono>
#include <functional>
#include <regex>
#include <thread>

#include <Streamer.hpp>
#include <librdkafka/rdkafkacpp.h>

#include <flatbuffers/flatbuffers.h>

#include "schemas/ev42_events_generated.h"
uint64_t event_timestamp(char *msg, int) {
  auto event = GetEventMessage(static_cast<const void *>(msg));
  int pid = event->message_id();
  uint64_t timestamp = event->pulse_time();
  std::cout << "\tpacket id : " << pid << "\ttimestamp : " << timestamp << "\n";
  return timestamp;
}

struct DummyAlgo {
  DummyAlgo(RdKafka::Topic *&t, const int &p) : topic(t), partition(p) {}

  uint64_t seek(const uint64_t &target, RdKafka::Consumer *consumer) {
    uint64_t offset = 0;
    uint64_t ts = 18446744073309970608u + 1u;
    if (!high) {
      consumer->query_watermark_offsets(topic->name(), partition, &low, &high,
                                        1000);
    }

    auto msg = consumer->consume(topic, partition, 1000);
    if (msg->err() != RdKafka::ERR_NO_ERROR) {
      std::cerr << "Failed to consume message: " << RdKafka::err2str(msg->err())
                << "\n";
    } else {
      ts = event_timestamp((char *)msg->payload(), -1);
      offset = msg->offset();
      std::cout << "current offset : " << offset
                << "\tcurrent timestamp : " << ts << "\n";
    }
    delete msg;

    //    auto prev = std::max(low, offset - 100);
    uint64_t prev;
    std::cout << "insert offset\n";
    std::cin >> prev;
    if (ts > target) {
      auto err = consumer->seek(topic, partition, prev, 5000);
      if (err != RdKafka::ERR_NO_ERROR) {
        std::cerr << "seek failed : " << RdKafka::err2str(err) << "\n";
        return ts;
      }

      return this->seek(target, consumer);
    }
    return ts;
  }

private:
  RdKafka::Topic *topic;
  int partition;
  int64_t low;
  int64_t high;
};

class MinimalProducer {
public:
  MinimalProducer(){};

  void SetUp() {
    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    std::string errstr;
    conf->set("metadata.broker.list", broker, errstr);
    _producer = RdKafka::Producer::create(conf, errstr);
    if (!_producer) {
      std::cerr << "Failed to create producer: " << errstr << std::endl;
      exit(1);
    }
    _topic = RdKafka::Topic::create(_producer, topic, tconf, errstr);

    _tp = RdKafka::TopicPartition::create(topic, _partition);

    if (!_topic) {
      std::cerr << "Failed to create topic: " << errstr << std::endl;
      exit(1);
    }
    _pause = true;
    _stop = true;
  }

  void TearDown() {
    if (t.joinable())
      stop();
    _producer->poll(1000);
    delete _tp;
    delete _topic;
    delete _producer;
  }

  void dr_cb(RdKafka::Message &message) { _message_count++; }

  void produce(std::string prefix = "message-") {
    int counter = 0;
    while (!_stop) {

      if (!_pause) {
        std::string line = prefix + std::to_string(counter);
        if (produce_single_message(line) != RdKafka::ERR_NO_ERROR) {
          std::cerr << "% Produce failed: " << std::endl;
          exit(1);
        }
        _producer->poll(0);
        ++counter;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return;
  }

  RdKafka::ErrorCode
  produce_single_message(std::string message = "no-message-0") {
    RdKafka::ErrorCode resp = _producer->produce(
        _topic, _partition, RdKafka::Producer::RK_MSG_COPY,
        const_cast<char *>(message.c_str()), message.size(), NULL, NULL);
    return resp;
  }

  void start() {
    _stop = false;
    _pause = false;
    t = std::thread([this] { this->produce(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  void pause() { _pause = true; }
  void stop() {
    _stop = true;
    t.join();
  }
  const int count() { return _message_count; }

  int32_t _partition = RdKafka::Topic::PARTITION_UA;
  int32_t _message_count = 0;
  RdKafka::Producer *_producer;
  RdKafka::Topic *_topic;
  RdKafka::TopicPartition *_tp;

  std::thread t;
  std::atomic<bool> _pause;
  std::atomic<bool> _stop;

  std::string broker;
  std::string topic;
};

class MinimalConsumer {
public:
  MinimalConsumer(){};

  void SetUp() {
    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

    std::string errstr;
    if (conf->set("metadata.broker.list", broker, errstr) !=
        RdKafka::Conf::CONF_OK) {
      throw std::runtime_error("Failed to initialise configuration: " + errstr);
    }
    if (conf->set("group.id", "1", errstr) != RdKafka::Conf::CONF_OK) {
      throw std::runtime_error("Failed to initialise configuration: " + errstr);
    }
    if (conf->set("api.version.request", "true", errstr) !=
        RdKafka::Conf::CONF_OK) {
      throw std::runtime_error("Failed to initialise configuration: " + errstr);
    }

    if (!(_consumer = RdKafka::Consumer::create(conf, errstr))) {
      throw std::runtime_error("Failed to create consumer: " + errstr);
    }
    delete conf;

    if (!(_topic =
              RdKafka::Topic::create(_consumer, topics[0], tconf, errstr))) {
      throw std::runtime_error("Failed to create topic: " + errstr);
    }

    RdKafka::ErrorCode resp =
        _consumer->start(_topic, _partition, RdKafka::Topic::OFFSET_END);
    if (resp != RdKafka::ERR_NO_ERROR) {
      throw std::runtime_error("Failed to create consumer: " + err2str(resp));
    }

    _consumer->query_watermark_offsets(topics[0], _partition, &low, &high,
                                       1000);
  }

  void TearDown() {
    _consumer->stop(_topic, _partition);
    delete _topic;
    delete _consumer;
  }

  void consume(std::function<uint64_t(char *, int)> f = [](char *, int) {
    return 0;
  }) {
    using namespace std::chrono;
    RdKafka::Message *msg;
    while (1) {
      msg = _consumer->consume(_topic, _partition, 1000);
      if (msg->err() != RdKafka::ERR_NO_ERROR) {
        std::cerr << "Failed to consume message: "
                  << RdKafka::err2str(msg->err()) << "\n";
      } else {
        std::cout << "> offset : " << msg->offset() << "\t-\tlen :\t"
                  << msg->len() << "\t-\tpayload :\t"
                  << (msg->len() > 0 ? std::string((char *)msg->payload()) : "")
                  << "\t-\ttimestamp :\t"
                  << ((msg->timestamp().type !=
                       RdKafka::MessageTimestamp::MSG_TIMESTAMP_NOT_AVAILABLE)
                          ? msg->timestamp().timestamp
                          : -1)
                  << "\n";
        _offset = msg->offset();
        f((char *)msg->payload(), -1);
      }
    }
    delete msg;
    return;
  }

  std::string consume_single_message() {
    RdKafka::Message *msg = _consumer->consume(_topic, _partition, 1000);
    if (msg->err() != RdKafka::ERR_NO_ERROR) {
      std::cerr << "Failed to consume message: " << RdKafka::err2str(msg->err())
                << "\n";
    } else {
      std::cout << "> offset : " << msg->offset() << "\t-\tlen :\t"
                << msg->len() << "\t-\tpayload :\t"
                << (msg->len() > 0 ? std::string((char *)msg->payload()) : "")
                << "\t-\ttimestamp :\t"
                << ((msg->timestamp().type !=
                     RdKafka::MessageTimestamp::MSG_TIMESTAMP_NOT_AVAILABLE)
                        ? msg->timestamp().timestamp
                        : -1)
                << "\n";
    }
    return std::string("");
  }

  template <typename A> void seek(const uint64_t target, A &m) {
    m.seek(target, _consumer);
  }

  int32_t _partition = 0;
  int32_t _message_count = 0;
  RdKafka::Consumer *_consumer;
  RdKafka::Topic *_topic;
  std::string broker;
  std::vector<std::string> topics;
  int64_t low, high;
  int64_t _offset;
};

class MinimalKafkaConsumer {
public:
  MinimalKafkaConsumer(){};

  void SetUp() {
    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

    std::string errstr;
    if (conf->set("metadata.broker.list", broker, errstr) !=
        RdKafka::Conf::CONF_OK) {
      throw std::runtime_error("Failed to initialise configuration: " + errstr);
    }
    if (conf->set("group.id", "1", errstr) != RdKafka::Conf::CONF_OK) {
      std::cerr << errstr << std::endl;
      exit(1);
    }
    if (conf->set("api.version.request", "true", errstr) !=
        RdKafka::Conf::CONF_OK) {
      throw std::runtime_error("Failed to initialise configuration: " + errstr);
    }

    if (!(_consumer = RdKafka::KafkaConsumer::create(conf, errstr))) {
      throw std::runtime_error("Failed to create consumer: " + errstr);
    }
    delete conf;

    build_partitions(5);
    set_topics_partition(topics[0]);
    RdKafka::ErrorCode err = _consumer->assign(_tp);

    // RdKafka::ErrorCode err = _consumer->subscribe(topics);
    if (err) {
      std::cerr << "Failed to subscribe to " << topics.size()
                << " topics: " << RdKafka::err2str(err) << std::endl;
      exit(1);
    }
  }

  void TearDown() {
    _consumer->close();
    delete _consumer;
  }

  int build_partitions(int retry) {
    RdKafka::Metadata *md;
    std::unique_ptr<RdKafka::Topic> ptopic;
    auto err =
        _consumer->metadata(ptopic.get() != NULL, ptopic.get(), &md, 5000);
    if (err != RdKafka::ERR_NO_ERROR) {
      fprintf(stderr, "Can't request metadata");
      if (retry) {
        build_partitions(retry - 1);
      } else {
        return -1;
      }
    }
    _metadata.reset(md);
    return 0;
  }

  int set_topics_partition(const std::string &topic) {
    bool found = false;
    if (!_metadata) {
      return -1;
    }

    auto partition_metadata = _metadata->topics()->at(0)->partitions();
    for (auto &i : *_metadata->topics()) {
      if (i->topic() == topic) {
        found = true;
        partition_metadata = i->partitions();
      }
    }
    if (!found) {
      fprintf(stderr, "Unable to find topic : %s\n", topic.c_str());
      return -1;
    }
    for (auto &i : (*partition_metadata)) {
      _tp.push_back(RdKafka::TopicPartition::create(topic, i->id()));
      fprintf(stderr, "%s : %d\n", topic.c_str(), i->id());
    }
    return 0;
  }

  void consume() {
    RdKafka::Message *msg;
    while (1) {
      msg = _consumer->consume(1000);
      if (msg->err() != RdKafka::ERR_NO_ERROR) {
        std::cerr << "Failed to consume message: "
                  << RdKafka::err2str(msg->err()) << "\n";
        //        break;
      } else {
        std::cout << "> offset : " << msg->offset() << "\t-\tlen :\t"
                  << msg->len() << "\t-\tpayload :\t"
                  << (msg->len() > 0 ? std::string((char *)msg->payload()) : "")
                  << "\t-\ttimestamp :\t"
                  << ((msg->timestamp().type !=
                       RdKafka::MessageTimestamp::MSG_TIMESTAMP_NOT_AVAILABLE)
                          ? msg->timestamp().timestamp
                          : -1)
                  << "\n";
      }
    }
    return;
  }

  std::string consume_single_message() {
    RdKafka::Message *msg = _consumer->consume(1000);
    if (msg->err() != RdKafka::ERR_NO_ERROR) {
      std::cerr << "Failed to consume message: " << RdKafka::err2str(msg->err())
                << "\n";
    } else {
      std::cout << "> offset : " << msg->offset() << "\t-\tlen :\t"
                << msg->len() << "\t-\tpayload :\t"
                << (msg->len() > 0 ? std::string((char *)msg->payload()) : "")
                << "\t-\ttimestamp :\t"
                << ((msg->timestamp().type !=
                     RdKafka::MessageTimestamp::MSG_TIMESTAMP_NOT_AVAILABLE)
                        ? msg->timestamp().timestamp
                        : -1)
                << "\n";
    }
    return std::string("");
  }

  void set_start_time(const uint64_t &timepoint) {
    std::map<std::string, int64_t> m;

    for (auto &i : _tp) {
      i->set_offset(timepoint);
    }

    auto err = _consumer->offsetsForTimes(_tp, 1000);
    if (err == RdKafka::ERR_NO_ERROR) {
      for (auto &i : _tp) {
        auto offset = i->offset();
        if (offset < 0) {
          //          LOG(3, "Unable to find required offset, start from last
          // message");
          fprintf(stderr,
                  "Unable to find required offset, start from last message\n");
        } else {
          fprintf(stderr, "Found offset : %ld\n", offset);
        }
        i->set_offset(offset);
      }
    }
    _consumer->assign(_tp);
  }
  int32_t _partition = 0;
  int32_t _message_count = 0;
  RdKafka::KafkaConsumer *_consumer;
  std::vector<RdKafka::TopicPartition *> _tp;
  std::shared_ptr<RdKafka::Metadata> _metadata;

  std::string broker;
  std::vector<std::string> topics;
};

std::function<FileWriter::ProcessMessageResult(void *, int)> verbose =
    [](void *x, int size) {
      std::cout << "message: " << std::string((char *)x) << std::endl;
      return FileWriter::ProcessMessageResult::OK();
    };

std::string time_diff_message;
std::function<FileWriter::TimeDifferenceFromMessage_DT(void *, int)> time_diff =
    [](void *x, int size) {
      std::smatch m;
      auto s = std::string((char *)x);
      std::cout << s << std::endl;
      std::regex_search(s, m, std::regex("[0-9]+$"));
      int time = std::atoi(std::string(m[0]).c_str());
      std::regex_search(s, m, std::regex("^[a-zA-Z]+"));
      time_diff_message = std::string(m[0]);

      return FileWriter::TimeDifferenceFromMessage_DT(time_diff_message, time);
    };

int main(int argc, char **argv) {

  using namespace FileWriter;

  MinimalProducer producer;
  MinimalKafkaConsumer consumer;

  producer.topic = "AMOR.area.detector";
  consumer.topics.push_back("AMOR.area.detector");
  consumer.broker = producer.broker = "ess01.psi.ch:9092";

  consumer.SetUp();

  // std::cout << "lower offset : \t" << consumer.low << "\t"
  //           << "higher offset : \t " << consumer.high << "\n";

  // DummyAlgo ds(consumer.topic, consumer._partition);
  // consumer.seek(18446744073309970608u, ds);
  uint64_t t;
  for (int i = 0; i < 10; ++i)
    consumer.consume_single_message();

  std::cin >> t;
  consumer.set_start_time(t);
  for (int i = 0; i < 10; ++i)
    consumer.consume_single_message();

  return 0;
}
