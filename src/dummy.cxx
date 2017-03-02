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
    delete _topic;
    delete _producer;
  }
  
  void dr_cb (RdKafka::Message &message) { _message_count++; }
  void produce(std::string prefix="message-") {
    std::string line;
    int counter=0;
    while(!_stop) {
      
      if(!_pause) {
	line = prefix+std::to_string(counter);
	RdKafka::ErrorCode resp =
	  _producer->produce(_topic, _partition,
			     RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
			     const_cast<char *>(line.c_str()), line.size(),
			     NULL, NULL);
	if (resp != RdKafka::ERR_NO_ERROR)
	  std::cerr << "% Produce failed: " <<
	    RdKafka::err2str(resp) << std::endl;
	_producer->poll(0);
	++counter;
      }
      std::this_thread::sleep_for (std::chrono::milliseconds(100));
    }
    return;
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
  std::thread t;
  std::atomic<bool> _pause;
  std::atomic<bool> _stop;

  std::string broker;
  std::string topic;
};


std::function<BrightnESS::FileWriter::ProcessMessageResult(void*,int)> verbose = [](void* x, int size) {
  std::cout << "message: " << std::string((char*)x) << std::endl;
  return BrightnESS::FileWriter::ProcessMessageResult::OK();
};



int main() {

  MinimalProducer producer;
  producer.topic="dummy";
  producer.broker="129.129.188.59:9092";
  BrightnESS::FileWriter::Streamer s( producer.broker, 
				      producer.topic,
				      BrightnESS::FileWriter::RdKafkaOffsetEnd);
  BrightnESS::FileWriter::DemuxTopic demux(producer.topic);
  

  producer.SetUp();
  producer.start();
  std::this_thread::sleep_for (std::chrono::milliseconds(1000));
  producer.stop();

  int counter =0;
  BrightnESS::FileWriter::ProcessMessageResult status = BrightnESS::FileWriter::ProcessMessageResult::OK();
  do {
    status = s.write(verbose);
    ++counter;
  } while(status.is_OK());

  BrightnESS::FileWriter::TimeDifferenceFromMessage_DT dt = s.jump_back(demux);
  do {

    status = s.write(verbose);
    ++counter;
  } while(status.is_OK());



  producer.TearDown();

  return 0;
}

























