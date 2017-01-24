#include <gtest/gtest.h>
#include <algorithm>
#include <stdexcept>

#include <StreamMaster.hpp>
#include <Streamer.hpp>
#include <librdkafka/rdkafkacpp.h>

class MockDemuxTopic : public MessageProcessor {
public:
  MockDemuxTopic(std::string topic) : _topic(topic) { };
  std::string const & topic() const { return _topic; };

  void process_message(char * msg_data, int msg_size) {
    std::string s(msg_data);
    int source_index=0;
    for(; source_index < _sources.size(); ++source_index) {
      if( (s.find(_sources[source_index]) !=std::string::npos ) ) {
        if( _sources[source_index]).process_message_counte
        // do something
        break;
      }
    }
    _message_counter[source_index]++;
  };
  
  void add_source(std::string s) {
    if (std::find (_sources.begin(), _sources.end(), s) == _sources.end()) {
      _sources.push_back(s);
      _message_counter.push_back(0);
    }
  }
  std::vector<std::string> & sources() { return _sources; };
  std::vector<int> & message_counter() { return _message_counter; };
  
private:
  std::string _topic;
  std::vector<std::string> _sources;
  std::vector<int> _message_counter;
};

template<>
int Streamer::write(MockDemuxTopic & mp) {
  
  RdKafka::Message *msg = consumer->consume(topic, partition, 1000);
  if( msg->err() == RdKafka::ERR__PARTITION_EOF) {
    std::cout << "eof reached" << std::endl;
    return RdKafka::ERR__PARTITION_EOF;
  }
  if( msg->err() != RdKafka::ERR_NO_ERROR) {
    std::cout << "Failed to consume message: "+RdKafka::err2str(msg->err()) << std::endl;
    return msg->err();
  }
  mp.process_message((char*)msg->payload(),msg->len());
  message_length = msg->len();
  return RdKafka::ERR_NO_ERROR;
}


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
  using StreamMaster=StreamMaster<Streamer,MockDemuxTopic>;
  EXPECT_THROW(StreamMaster sm(broker,no_demux), std::runtime_error);
}

TEST (Streammaster, Constructor) {
  using StreamMaster=StreamMaster<Streamer,MockDemuxTopic>;
  EXPECT_NO_THROW(StreamMaster sm(broker,demux));
}


TEST (Streammaster, StartStop) {
  using StreamMaster=StreamMaster<Streamer,MockDemuxTopic>;
  EXPECT_TRUE( one_demux[0].sources().size()  == 0 );
  one_demux[0].add_source("for_example_motor01");
  one_demux[0].add_source("for_example_temperature02");
  EXPECT_FALSE( one_demux[0].sources().size()  == 0 );

  StreamMaster sm(broker,one_demux);
  bool is_joinable = sm.start();
  EXPECT_TRUE( is_joinable );
  std::this_thread::sleep_for (std::chrono::seconds(10));
  auto value = sm.stop();

  for(int item=0;item<one_demux[0].sources().size();++item)
    std::cout << one_demux[0].sources()[item] <<  " : " << one_demux[0].message_counter()[item] << "\n";

}



// TEST (Streammaster, PollNMessages) {
//   using StreamMaster=StreamMaster<Streamer,MockDemuxTopic>;

//   StreamMaster sm(broker,one_demux);
//   sm.poll_n_messages(10);

  
//   // bool is_joinable = sm.start();
//   // EXPECT_TRUE( is_joinable );
//   std::this_thread::sleep_for (std::chrono::seconds(10));
//   auto value = sm.stop();

//   // std::for_each(value.begin(),value.end(),[](auto& item) {
//   //     std::cout << item.first << ":" << item.second << std::endl;
//   //     EXPECT_TRUE(item.second == 0);
//   //   });
// }




int main(int argc, char **argv) {

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
