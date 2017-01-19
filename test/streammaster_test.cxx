#include <gtest/gtest.h>
#include <algorithm>
#include <stdexcept>

#include <StreamMaster.hpp>
#include <Streamer.hpp>
#include <DemuxTopic.h>

std::string broker;
std::vector<std::string> topic = {"area_detector","tof_detector","motor1","motor2","temp"};


TEST (Streamer, NotAllocatedFailure) {

  StreamMaster<Streamer,DemuxTopic> sm(broker,topic);

  sm.start();
  std::this_thread::sleep_for (std::chrono::seconds(1));
  sm.stop();
  
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
