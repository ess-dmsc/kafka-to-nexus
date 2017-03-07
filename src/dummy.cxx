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
    if (conf->set("group.id", "1", errstr) != RdKafka::Conf::CONF_OK) {
      std::cerr << errstr << std::endl;
      exit(1);
    }
    if (!debug.empty()) {
      if (conf->set("debug", debug, errstr) != RdKafka::Conf::CONF_OK) {
	throw std::runtime_error("Failed to initialise configuration: " + errstr);
      }
    }

    if (!(_consumer = RdKafka::KafkaConsumer::create(conf, errstr))) {
      throw std::runtime_error("Failed to create consumer: " + errstr);
    }
    delete conf;
    RdKafka::ErrorCode err = _consumer->subscribe(topics);
    if (err) {
      std::cerr << "Failed to subscribe to " << topics.size() << " topics: "
		<< RdKafka::err2str(err) << std::endl;
      exit(1);
    }
    if (!(_topic = RdKafka::Topic::create(_consumer, topics[0], tconf,errstr))) {
      throw std::runtime_error("Failed to create topic: " + errstr);
    }

  }
  
  void TearDown() {
    _consumer->close();
    delete _topic;
    delete _consumer;
  }

  void consume() {
    RdKafka::Message *msg;
    while(1) {
      msg = _consumer->consume(1000);
      if (msg->err() != RdKafka::ERR_NO_ERROR) {
	std::cerr << "Failed to consume message: " 
		  << RdKafka::err2str(msg->err())  << "\n";
	break;
      }
      else {
	std::cout << "> offset : " << msg->offset()
		  << "\t-\tlen :\t" << msg->len()
		  << "\t-\tpayload :\t" << (msg->len() > 0 ? std::string((char*)msg->payload()) : "")
		  << "\n";
      }
    } 
    return;
  }

  std::string consume_single_message() {
    RdKafka::Message *msg =
      _consumer->consume(1000);
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
  RdKafka::KafkaConsumer *_consumer;
  RdKafka::Topic *_topic;

  std::string broker;
  std::vector<std::string> topics;

};


std::function<BrightnESS::FileWriter::ProcessMessageResult(void*,int)> verbose = [](void* x, int size) {
  std::cout << "message: " << std::string((char*)x) << std::endl;
  return BrightnESS::FileWriter::ProcessMessageResult::OK();
};



int main(int argc, char **argv) {

  using namespace BrightnESS::FileWriter;

  MinimalProducer producer;
  MinimalConsumer consumer;

  producer.topic="dummy";
  consumer.topics.push_back(producer.topic);
  consumer.broker=producer.broker="129.129.188.59:9092";
  
  if (argc > 1) {
    producer.SetUp();
    // producer.start();
    // std::this_thread::sleep_for (std::chrono::milliseconds(1000));
    // producer.stop();
    for( int i = 0;i < 100; ++i) {
      std::string line = "hello"+std::to_string(i);
      producer.produce_single_message(line);
    }
    producer.TearDown();
  }
  else {
  //   consumer.SetUp();

  //   ExampleRebalanceCb erc;


  //   std::vector<RdKafka::TopicPartition*> partitions;
  //   consumer._consumer->assignment(partitions);
  //   for(auto& tp : partitions) {
  //     std::cout << "Partition: "    << tp->partition() << "\n";
  //     std::cout << "\ttopic() :\t"  << tp->topic()  << "\n"; 
  //     std::cout << "\toffset() :\t" << tp->offset()  << "\n"; 
  //     std::cout << "\terr() :\t"    << tp->err()  << "\n\n"; 
  //   }
  //   std::vector<RdKafka::TopicPartition*> p;
  //   p.push_back(RdKafka::TopicPartition::create(producer.topic, 
  // 						1, 
  // 						RdKafka::Topic::OFFSET_BEGINNING));
  //   RdKafka::ErrorCode err = consumer._consumer->assign(p);
  //   erc.rebalance_cb(consumer._consumer,err,p);
  //   std::cerr << RdKafka::err2str(err)  << "\n";
  //   if (err != RdKafka::ERR_NO_ERROR) {
  //     exit(1);
  //   }
  //   consumer.consume(); 
  //   consumer._consumer->unassign();

  //   consumer._consumer->assign(partitions);
  //   consumer._consumer->assignment(partitions);
  //   for(auto& tp : partitions) {
  //     std::cout << "Partition: "    << tp->partition() << "\n";
  //     std::cout << "\ttopic() :\t"  << tp->topic()  << "\n"; 
  //     std::cout << "\toffset() :\t" << tp->offset()  << "\n"; 
  //     std::cout << "\terr() :\t"    << tp->err()  << "\n\n"; 
  //   }
  //   std::cout << consumer.consume_single_message() << "\n"; 

  //   p.pop_back();
  //   p.push_back(RdKafka::TopicPartition::create(producer.topic, 
  // 						1, 
  // 						RdKafka::Topic::OFFSET_END));
  //   err = consumer._consumer->assign(p);
  //   // erc.rebalance_cb(consumer._consumer,err,consumer._tp);
  //   std::cerr << RdKafka::err2str(err)  << "\n";
  //   if (err != RdKafka::ERR_NO_ERROR) {
  //     exit(1);
  //   }
  //   consumer._consumer->assignment(partitions);
  //   for(auto& tp : partitions) {
  //     std::cout << "Partition: "    << tp->partition() << "\n";
  //     std::cout << "\ttopic() :\t"  << tp->topic()  << "\n"; 
  //     std::cout << "\toffset() :\t" << tp->offset()  << "\n"; 
  //     std::cout << "\terr() :\t"    << tp->err()  << "\n\n"; 
  //   }
  //   std::cout << consumer.consume_single_message() << "\n"; 

  //   // std::cout << consumer.consume_single_message() << "\n"; 

  //   p.pop_back();
  //   p.push_back(RdKafka::TopicPartition::create(producer.topic, 
  // 						1, 
  // 						RdKafka::Topic::OFFSET_STORED));
  //   err = consumer._consumer->assign(p);
  //   // erc.rebalance_cb(consumer._consumer,err,consumer._tp);
  //   std::cerr << RdKafka::err2str(err)  << "\n";
  //   if (err != RdKafka::ERR_NO_ERROR) {
  //     exit(1);
  //   }
  //   consumer._consumer->assignment(partitions);
  //   for(auto& tp : partitions) {
  //     std::cout << "Partition: "    << tp->partition() << "\n";
  //     std::cout << "\ttopic() :\t"  << tp->topic()  << "\n"; 
  //     std::cout << "\toffset() :\t" << tp->offset()  << "\n"; 
  //     std::cout << "\terr() :\t"    << tp->err()  << "\n\n"; 
  //   }
  //   std::cout << consumer.consume_single_message() << "\n"; 

  //   // std::cout << consumer.consume_single_message() << "\n"; 

  //   int in = 1;
  //   while(1) {
  //     std::cin >> in;
  //     if (in < 0) break;
  //     p.pop_back();
  //     p.push_back(RdKafka::TopicPartition::create(producer.topic, 
  // 						  1,
  // 						  RdKafka::Consumer::OffsetTail(in)
  //     ));
  //     err = consumer._consumer->assign(p);
  //     erc.rebalance_cb(consumer._consumer,err,p);
  //     std::cerr << RdKafka::err2str(err)  << "\n";
  //     if (err != RdKafka::ERR_NO_ERROR) {
  // 	exit(1);
  //     }
  //     consumer._consumer->assignment(p);
  //     for(auto& tp : p) {
  // 	std::cout << "Partition: "    << tp->partition() << "\n";
  // 	std::cout << "\ttopic() :\t"  << tp->topic()  << "\n"; 
  // 	std::cout << "\toffset() :\t" << tp->offset()  << "\n"; 
  // 	std::cout << "\terr() :\t"    << tp->err()  << "\n\n"; 
  //     }
  //     consumer.consume(); 
  //   }

  //   consumer.TearDown();
  // }

    Streamer s(producer.broker,producer.topic,RdKafkaOffsetEnd);
    DemuxTopic demux(producer.topic);

    int counter =0;
    ProcessMessageResult status = ProcessMessageResult::OK();
    do {
      status = s.write(verbose);
      ++counter;
    } while(status.is_OK());

    std::cout << "first available offset : \t" << s._begin_offset.value() << std::endl;
    std::cout << "current offset : \t" << s._offset.value() << std::endl;
    
    // TimeDifferenceFromMessage_DT dt = 
      s.jump_back(demux,10);
    // TimeDifferenceFromMessage_DT dt = 
      s.jump_back(demux,10);
      s.jump_back(demux,1000);

    // do {
    //   status = s.write(verbose);
    //   ++counter;
    // } while(status.is_OK());
    
  }  
  return 0;
}

























