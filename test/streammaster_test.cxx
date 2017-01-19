#include <gtest/gtest.h>
#include <algorithm>
#include <stdexcept>

#include <StreamMaster.hpp>
#include <Streamer.hpp>
#include <DemuxTopic.h>

std::string broker;
std::vector<std::string> topic = {"area_detector","tof_detector","motor1","motor2","temp"};
std::vector<std::string> no_topic = {""};


TEST (Streamer, NotAllocatedFailure) {
  using StreamMaster=StreamMaster<Streamer,DemuxTopic>;
  
  EXPECT_THROW(StreamMaster sm(broker,no_topic), std::runtime_error);
  StreamMaster sm;
  EXPECT_THROW(sm.push_back(broker,no_topic[0]), std::runtime_error);
}

TEST (Streamer, Constructor) {
  using StreamMaster=StreamMaster<Streamer,DemuxTopic>;

  EXPECT_NO_THROW(StreamMaster sm(broker,topic));
  StreamMaster sm;
  EXPECT_NO_THROW(sm.push_back(broker,"new_motor"));

}


TEST (Streamer, StartStop) {
  using StreamMaster=StreamMaster<Streamer,DemuxTopic>;

  StreamMaster sm(broker,topic);
  bool is_joinable = sm.start();
  EXPECT_TRUE( is_joinable );
  std::this_thread::sleep_for (std::chrono::seconds(1));
  auto value = sm.stop();
  std::for_each(value.begin(),value.end(),[](auto& item) {
      //      std::cout << item.first << ":" << item.second << std::endl;
      EXPECT_TRUE(item.second == 0);
    });
}




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
