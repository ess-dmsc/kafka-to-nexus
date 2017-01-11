#include <gtest/gtest.h>
#include <algorithm>
#include <stdexcept>

#include <StreamMaster.hpp>
#include <Streamer.hpp>

std::string broker;
std::string topic;


TEST (Streamer, NotAllocatedFailure) {
  StreamMaster<Streamer> sm;
  EXPECT_FALSE( sm.status() );
  sm.push_back("168.0.0.1","test","first");
  EXPECT_TRUE( sm.status() );
  int i=1;
  sm.run<int>(i);
}





int main(int argc, char **argv) {

  ::testing::InitGoogleTest(&argc, argv);
  for(int i=1;i<argc;++i) {
    std::string opt(argv[i]);
    size_t found = opt.find("=");
    if( opt.substr(0,found) == "--kafka_broker")
      broker = opt.substr(found+1);
    if( opt.substr(0,found) ==  "--kafka_topic")
      topic = opt.substr(found+1);
    if( opt.substr(0,found) ==  "--help" ) {
      std::cout << "\nOptions: " << "\n"
                << "\t--kafka_broker=<host>:<port>[default = 9092]\n"
                << "\t--kafka_topic=<topic>\n";
    }
  }

  return RUN_ALL_TESTS();
}
