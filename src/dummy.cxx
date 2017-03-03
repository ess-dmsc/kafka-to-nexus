#include <thread>
#include <atomic>

#include <Streamer.hpp>
#include <librdkafka/rdkafkacpp.h>

class MinimalProducer {
public:
  MinimalProducer() {};

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
    _topic = RdKafka::Topic::create(_producer, topic,
				    tconf, errstr);

    _tp = RdKafka::TopicPartition::create(topic,_partition);

    if (!_topic) {
      std::cerr << "Failed to create topic: " << errstr << std::endl;
      exit(1);
    }
    _pause = true;
    _stop = true;
  }  
  
  void TearDown() {
    if( t.joinable() ) stop();
    _producer->poll(1000);
    delete _tp;
    delete _topic;
    delete _producer;
  }
  
  void dr_cb (RdKafka::Message &message) { _message_count++; }

  void produce(std::string prefix="message-") {
    int counter=0;
    while(!_stop) {
      
      if(!_pause) {
	std::string line = prefix+std::to_string(counter);
	if (produce_single_message(line) != RdKafka::ERR_NO_ERROR) {
	  std::cerr << "% Produce failed: " << std::endl;
	  exit(1);
	}
	_producer->poll(0);
	++counter;
      }
      std::this_thread::sleep_for (std::chrono::milliseconds(10));
    }
    return;
  }


  RdKafka::ErrorCode produce_single_message(std::string message="no-message-0") {
    RdKafka::ErrorCode resp =
      _producer->produce(_topic, _partition,
			 RdKafka::Producer::RK_MSG_COPY,
			 const_cast<char *>(message.c_str()), message.size(),
			 NULL, NULL);
    return resp;
  }
  
  
  void start() {
    _stop = false;
    _pause = false;
    t = std::move( std::thread( [this] { this->produce(); } ) );
    std::this_thread::sleep_for (std::chrono::milliseconds(100));
  }
  void pause() { _pause = true; }
  void stop() { _stop = true;  t.join(); }
  const int count() { return _message_count; }

  int32_t _partition = RdKafka::Topic::PARTITION_UA;
  int32_t _message_count=0;
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

  MinimalConsumer() { };
  
  void SetUp() {
    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    
    std::string debug, errstr;
    if (conf->set("metadata.broker.list", broker, errstr) !=
	RdKafka::Conf::CONF_OK) {
      throw std::runtime_error("Failed to initialise configuration: " + errstr);
    }
    if (!debug.empty()) {
      if (conf->set("debug", debug, errstr) != RdKafka::Conf::CONF_OK) {
	throw std::runtime_error("Failed to initialise configuration: " + errstr);
      }
    }

    if (!(_consumer = RdKafka::Consumer::create(conf, errstr))) {
      throw std::runtime_error("Failed to create consumer: " + errstr);
    } 
    if (!(_topic = RdKafka::Topic::create(_consumer, topic, tconf,errstr))) {
      throw std::runtime_error("Failed to create topic: " + errstr);
    }

    _tp = RdKafka::TopicPartition::create(topic,_partition);
    
    // Start consumer for topic+partition at start offset
    RdKafka::ErrorCode resp = _consumer->start(_topic,_partition,RdKafka::Topic::OFFSET_BEGINNING);
    if (resp != RdKafka::ERR_NO_ERROR) {
      throw std::runtime_error("Failed to start consumer: " +
			       RdKafka::err2str(resp));
    }
  }
  
  void TearDown() {
    _consumer->stop(_topic,_partition);
    delete _topic;
    delete _consumer;
  }

  std::string consume_single_message() {
    RdKafka::Message *msg =
      _consumer->consume(_topic,_partition,1000);
    if (msg->err() != RdKafka::ERR_NO_ERROR) {
      std::cerr << "Failed to consume message: " 
    		<< RdKafka::err2str(msg->err())  << "\n";
    }
    else {
      std::cout << "> offset : " << msg->offset()
    		<< "\t-\tlen :\t" << msg->len()
		<< "\t-\tpayload :\t" << (msg->len() > 0 ? std::string((char*)msg->payload()) : "")
    		<< "\n";
    }
    return std::string("");
  }



  int32_t _partition = 0;
  int32_t _message_count=0;
  RdKafka::Consumer *_consumer;
  RdKafka::Topic *_topic;
  RdKafka::TopicPartition *_tp;

  std::string broker;
  std::string topic;

};













std::function<BrightnESS::FileWriter::ProcessMessageResult(void*,int)> verbose = [](void* x, int size) {
  std::cout << "message: " << std::string((char*)x) << std::endl;
  return BrightnESS::FileWriter::ProcessMessageResult::OK();
};



int main(int argc, char **argv) {

  using namespace BrightnESS::FileWriter;

  MinimalProducer producer;
  MinimalConsumer consumer;

  consumer.topic=producer.topic="dummy";
  consumer.broker=producer.broker="129.129.188.59:9092";
  
  if (argc > 1) {
    producer.SetUp();
    producer.start();
    std::this_thread::sleep_for (std::chrono::milliseconds(1000));
    producer.stop();
    // producer.produce_single_message("hello-1");
    producer.TearDown();
  }
  else {
    consumer.SetUp();

    std::cout << consumer.consume_single_message() << "\n"; 
    consumer._tp->set_offset(RdKafka::Consumer::OffsetTail(2550));
    std::cout << "set offset: 0" << std::endl;    
    std::cout << consumer.consume_single_message() << "\n"; 

    // consumer._tp->set_offset(0);
    // std::cout << "set offset: 1" << std::endl;    
    // std::cout << consumer.consume_single_message() << "\n"; 
    

    // consumer.TearDown();
  }

  // int counter =0;
  // ProcessMessageResult status = ProcessMessageResult::OK();
  // do {
  //   status = s.write(verbose);
  //   ++counter;
  // } while(status.is_OK());
  
  // TimeDifferenceFromMessage_DT dt = s.jump_back(demux);
  // do {
  //   status = s.write(verbose);
  //   ++counter;
  // } while(status.is_OK());
  
  return 0;
}

























