#include <gtest/gtest.h>
#include <algorithm>
#include <stdexcept>
#include <thread>
#include <chrono>
#include <atomic>


#include <Streamer.hpp>
#include <librdkafka/rdkafkacpp.h>

const int max_recv_messages = 10;

using namespace BrightnESS::FileWriter;


class MinimalProducer : public ::testing::Test {

protected:

  virtual void SetUp() {
    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    std::string errstr;
    conf->set("metadata.broker.list", broker, errstr);
    //    conf->set("dr_cb", &MinimalProducer::dr_cb, errstr);

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

  virtual void TearDown() {
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
	// else
	//   std::cerr << "% Produced message (" << line.size() << " bytes)" << std::endl;
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
public:
  static std::string broker;
  static std::string topic;
};

std::string MinimalProducer::broker = "localhost";
std::string MinimalProducer::topic = "streamer-test-topic";


std::function<ProcessMessageResult(void*,int)> silent = [](void* x, int) { return ProcessMessageResult::OK(); };

std::function<ProcessMessageResult(void*,int)> verbose = [](void* x, int size) {
  std::cout << "message: " << x << "\t" << size << std::endl;
  return ProcessMessageResult::OK();
};
std::function<ProcessMessageResult(void*,int,int*)> sum = [](void* x, int size, int* data_size) {
  (*data_size) += size;
  return ProcessMessageResult::OK();
};



std::pair<std::string,int64_t> dummy_message_parser(std::string&& msg) {
  auto position = msg.find("-", 0 );
  int64_t timestamp;
  if ( position != std::string::npos) {
    std::stringstream(msg.substr(position+1)) >> timestamp;
    return std::pair<std::string,int64_t>(msg.substr(0,position),timestamp);
  }
  // what to return??  make it an error.
  return std::pair<std::string,int64_t>("", -1);
}
std::function<TimeDifferenceFromMessage_DT(void*,int)> time_difference = [](void* x, int size) {
  auto parsed_text = dummy_message_parser(std::move(std::string((char*)x)));
  return TimeDifferenceFromMessage_DT(parsed_text.first,parsed_text.second);
};


TEST (Streamer, MissingTopicFailure) {
  ASSERT_THROW(Streamer(std::string("data_server:1234"),std::string("")),std::runtime_error);
}

TEST (Streamer, ConstructionSuccess) {
  ASSERT_NO_THROW(Streamer(MinimalProducer::broker,MinimalProducer::topic));
}

TEST (Streamer, NoReceive) {
  Streamer s(MinimalProducer::broker,"dummy_topic");
  using namespace std::placeholders;
  int data_size=0,counter =0;
  std::function<ProcessMessageResult(void*,int)> f1 = std::bind (sum,_1,_2,&data_size); 
  ProcessMessageResult status = ProcessMessageResult::OK();
  
  do {
    EXPECT_TRUE(status.is_OK());
    status = s.write(f1);
    ++counter;
  } while(status.is_OK()  && (counter < max_recv_messages ));
  
  EXPECT_EQ(data_size,0);
}


TEST_F (MinimalProducer, Receive) {
  using namespace std::placeholders;

  Streamer s(MinimalProducer::broker,MinimalProducer::topic);
  int data_size=0,counter =0;
  std::function<ProcessMessageResult(void*,int)> f1 = std::bind (sum,_1,_2,&data_size); 
  
  start();

  ProcessMessageResult status = ProcessMessageResult::OK();
  status = s.write(silent);
  ASSERT_TRUE(status.is_OK());
  
  do {
    EXPECT_TRUE(status.is_OK());
    status = s.write(f1);
    ++counter;
  } while(status.is_OK()  && (counter < max_recv_messages ));
  EXPECT_GT(data_size,0);

  stop();
}

TEST_F (MinimalProducer, Reconnect) {
  using namespace std::placeholders;
    
  int data_size=0,counter =0;
  std::function<ProcessMessageResult(void*,int)> f1 = std::bind (sum,_1,_2,&data_size); 

  start();

  Streamer s(MinimalProducer::broker,"dummy_topic");
  ProcessMessageResult status = ProcessMessageResult::OK();
  do {
    EXPECT_TRUE(status.is_OK());
    status = s.write(f1);
    ++counter;
  } while(status.is_OK() && (counter < max_recv_messages ));
  EXPECT_FALSE(data_size > 0);  
  EXPECT_EQ(s.disconnect(),0);

  data_size=0;
  counter=0; 
  EXPECT_EQ(s.connect(MinimalProducer::broker,MinimalProducer::topic),0);
  status = ProcessMessageResult::OK();
  do {
    EXPECT_TRUE(status.is_OK());
    status = s.write(f1);
    ++counter;
  } while(status.is_OK() && (counter < max_recv_messages ));
  EXPECT_TRUE(data_size > 0);  
  stop();
}

TEST_F (MinimalProducer, JumpBack) {
  Streamer s(broker,topic);
  ProcessMessageResult status = ProcessMessageResult::OK();
  int counter=0;
  start();
  do {
    EXPECT_TRUE(status.is_OK());
    status = s.write(silent);
    ++counter;
  } while(status.is_OK()  && (counter < max_recv_messages ));

  std::cout << "\n\nhello\n" << std::endl;
  DemuxTopic demux(MinimalProducer::topic);
  stop();
}


int main(int argc, char **argv) {


  ::testing::InitGoogleTest(&argc, argv);
  for(int i=1;i<argc;++i) {
    std::string opt(argv[i]);
    size_t found = opt.find("=");
    if( opt.substr(0,found) == "--kafka_broker")
      MinimalProducer::broker = opt.substr(found+1);
    if( opt.substr(0,found) ==  "--kafka_topic")
      MinimalProducer::topic = opt.substr(found+1);
    if( opt.substr(0,found) ==  "--help" ) {
      std::cout << "\nOptions: " << "\n"
                << "\t--kafka_broker=<host>:<port>[default = 9092]\n"
                << "\t--kafka_topic=<topic>\n";
    }

  }

  
  return RUN_ALL_TESTS();
}






