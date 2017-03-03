#include <gtest/gtest.h>
#include <algorithm>
#include <stdexcept>
#include <random>
#include<cstdint>

#include <StreamMaster.hpp>
#include <Streamer.hpp>
#include <librdkafka/rdkafkacpp.h>

//using namespace BrightnESS::FileWriter;

static std::mt19937_64 rng;



namespace BrightnESS {
  namespace FileWriter {
    class MockSource {
    public:
      MockSource(std::string topic, std::string source) : _topic(topic), _source(source) { };
      //  MockSource(MockSource &&);
      std::string const & topic() const { return _topic; }
      std::string const & source() const { return _source; }
      uint32_t processed_messages_count() const { return _processed_messages_count; }
      ProcessMessageResult process_message(char * msg_data, int msg_size) {
	_processed_messages_count++;
	int64_t value = rng();
	return ProcessMessageResult::OK(); }
    private:
      std::string _topic;
      std::string _source;
      int _processed_messages_count=0;
    };
    
    
    class MockDemuxTopic : public MessageProcessor {
    public:
      MockDemuxTopic(std::string topic) : _topic(topic) { };
      std::string const & topic() const { return _topic; };
      
      ProcessMessageResult process_message(char * msg_data, int msg_size) {
	std::string s(msg_data);
	int source_index=0;
	for(; source_index < _sources.size(); ++source_index) {
	  if( (s.find(_sources[source_index].source()) !=std::string::npos ) ) break;
	}
	return _sources[source_index].process_message(msg_data,msg_size);
      };
      
      void add_source(std::string s) {
	for(auto item=_sources.begin(); item != _sources.end(); ++item) {
	  if ( (*item).source() == s ) return;
	}
	_sources.push_back( MockSource(topic(),s) );
      }
      std::vector<MockSource> & sources() { return _sources; };
      
    private:
      std::string _topic;
      std::vector<MockSource> _sources;
      std::vector<int> _message_counter;
    };



template<>
BrightnESS::FileWriter::ProcessMessageResult BrightnESS::FileWriter::Streamer::write(MockDemuxTopic & mp) {

  RdKafka::Message *msg = consumer->consume(topic, partition, consumer_timeout.count());
  if( msg->err() == RdKafka::ERR__PARTITION_EOF) {
    //    std::cout << "eof reached" << std::endl;
    return ProcessMessageResult::OK();
  }
  if( msg->err() != RdKafka::ERR_NO_ERROR) {
    //    std::cout << "Failed to consume message: "+RdKafka::err2str(msg->err()) << std::endl;
    return ProcessMessageResult::ERR();
  }
  message_length = msg->len();
  return mp.process_message((char*)msg->payload(),msg->len());
}

  } // FileWriter
} // BrightnESS


using namespace BrightnESS::FileWriter;

std::string broker;
std::vector<std::string> no_topic = {""};
std::vector<std::string> topic = {"area_detector","tof_detector","motor1","motor2","temp"};


#if 1
std::vector<MockDemuxTopic> no_demux = { MockDemuxTopic("") };
std::vector<MockDemuxTopic> one_demux = { MockDemuxTopic("topic.with.multiple.sources") };
std::vector<MockDemuxTopic> demux = { MockDemuxTopic("area_detector"),
                                      MockDemuxTopic("tof_detector"),
                                      MockDemuxTopic("motor1"),
                                      MockDemuxTopic("motor2"),
                                      MockDemuxTopic("temp") };

TEST (Streammaster, NotAllocatedFailure) {
  using StreamMaster=StreamMaster<BrightnESS::FileWriter::Streamer,MockDemuxTopic>;
  EXPECT_THROW(StreamMaster sm(broker,no_demux), std::runtime_error);
}

TEST (Streammaster, Constructor) {
  using StreamMaster=StreamMaster<BrightnESS::FileWriter::Streamer,MockDemuxTopic>;
  EXPECT_NO_THROW(StreamMaster sm(broker,demux));
}


TEST (Streammaster, StartStop) {
  using StreamMaster=StreamMaster<BrightnESS::FileWriter::Streamer,MockDemuxTopic>;
  EXPECT_TRUE( one_demux[0].sources().size()  == 0 );
  one_demux[0].add_source("for_example_motor01");
  one_demux[0].add_source("for_example_temperature02");
  EXPECT_FALSE( one_demux[0].sources().size()  == 0 );

  StreamMaster sm(broker,one_demux);
  bool is_joinable = sm.start();
  EXPECT_TRUE( is_joinable );
  std::this_thread::sleep_for (std::chrono::seconds(1));
  auto value = sm.stop(-5);

  for(int item=0;item<one_demux[0].sources().size();++item)
    std::cout << one_demux[0].sources()[item].source() <<  " : "
              << one_demux[0].sources()[item].processed_messages_count() << "\n";
  
  StreamMaster::delay_after_last_message = 1_ms;
  
}





int main(int argc, char **argv) {
  rng.seed(1234);
  ::testing::InitGoogleTest(&argc, argv);
  for(int i=1;i<argc;++i) {
    std::string opt(argv[i]);
    size_t found = opt.find("=");
    if( opt.substr(0,found) == "--kafka_broker")
      broker = opt.substr(found+1);
    // if( opt.substr(0,found) ==  "--kafka_topic")
    //   topic = opt.substr(found+1);
    if( opt.substr(0,found) ==  "--help" ) {
      std::cout << "\nOptions: " << "\n"
                << "\t--kafka_broker=<host>:<port>[default = 9092]\n"
                // << "\t--kafka_topic=<topic>\n"
        ;
    }
  }

  return RUN_ALL_TESTS();
}

#else
int main(int argc, char **argv) {
}
#endif
