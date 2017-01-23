#include <gtest/gtest.h>
#include <algorithm>
#include <stdexcept>

#include <StreamMaster.hpp>
#include <Streamer.hpp>
#include <FileWriterTask.h>

std::string broker;
std::vector<std::string> no_topic = {""};
std::vector<std::string> topic = {"area_detector","tof_detector","motor1","motor2","temp"};
#if 0
std::vector<DemuxTopic> no_demux = { DemuxTopic("") };
std::vector<DemuxTopic> demux = { DemuxTopic("area_detector"),
                                  DemuxTopic("tof_detector"),
                                  DemuxTopic("motor1"),
                                  DemuxTopic("motor2"),
                                  DemuxTopic("temp") };

TEST (Streamer, NotAllocatedFailure) {
  using StreamMaster=StreamMaster<Streamer,DemuxTopic>;
  EXPECT_THROW(StreamMaster sm(broker,no_demux), std::runtime_error);
}

TEST (Streamer, Constructor) {
  using StreamMaster=StreamMaster<Streamer,DemuxTopic>;
  EXPECT_NO_THROW(StreamMaster sm(broker,demux));
}


TEST (Streamer, StartStop) {
  using StreamMaster=StreamMaster<Streamer,DemuxTopic>;

  StreamMaster sm(broker,demux);
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

#endif
int main(int argc, char **argv) {
}
