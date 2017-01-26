#include <gtest/gtest.h>
#include <algorithm>
#include <stdexcept>

#include <Streamer.hpp>
#include <librdkafka/rdkafkacpp.h>

std::string broker("129.168.10.11:9092");
std::string topic("test");

using namespace BrightnESS::FileWriter;

std::function<ProcessMessageResult(void*,int)> silent = [](void* x, int) { return ProcessMessageResult::OK(); };

std::function<ProcessMessageResult(void*,int)> verbose = [](void* x, int size) {
  std::cout << std::string((char*)x) << "\t" << size << std::endl;
  return ProcessMessageResult::OK();
};

TEST (Streamer, MissingTopicFailure) {
  ASSERT_THROW(Streamer(std::string("data_server:1234"),std::string("")),std::runtime_error);
}

TEST (Streamer, ConstructionSuccess) {
  ASSERT_NO_THROW(Streamer(broker,topic));
}

TEST (Streamer, NoReceive) {
  Streamer s(broker,topic+"_no");
  int f;
  EXPECT_FALSE( s.write(f).is_OK() ) ;
  EXPECT_FALSE( s.write(silent).is_OK() );
  EXPECT_TRUE( s.len() == 0 );
}


TEST (Streamer, Receive) {
  Streamer s(broker,topic);
  int data_size=0;
  std::function<void(void*,int)> f1 = [&](void* x, int size) {
    //    std::cout << std::string((char*)x) << "\t" << size << std::endl;
    data_size += size;
    return; };

  ProcessMessageResult status = ProcessMessageResult::OK();
  // .. warm up .. 
  s.write(silent);
  do {
    EXPECT_TRUE(status.is_OK());
    status = s.write(f1);
  } while(status.is_OK() );
  EXPECT_GT(data_size,0);
  std::cout << "data_size" << "\t" << data_size << std::endl;

}

TEST (Streamer, Reconnect) {
  int data_size=0;
  std::function<void(void*,int)> f1 = [&](void* x, int size) {
    data_size += size;
    return; };
  
  Streamer s(broker,topic);
  EXPECT_EQ(s.disconnect(),0);
  EXPECT_EQ(s.connect(broker,topic),0);
  for(int i=0;i<10;++i)
    s.write(f1);
  EXPECT_GT(data_size,0);
  //  std::cout << "data_size = " << data_size<< "\n";
}


TEST (Streamer, SearchBackward) {
  Streamer s(broker,topic);
  ProcessMessageResult status = ProcessMessageResult::OK();
  int last_value, new_value;
  std::function<void(void*,int)> register_value = [&](void* x, int) {
    last_value = *(int*)x;
  };
  std::function<void(void*,int)> rollback_value = [&](void* x, int) {
    new_value = *(int*)x;
  };
  
  do {
    status = s.write(register_value);
  } while(status.is_OK());
  
  s.search_backward(silent);
  status = s.write(rollback_value);

  std::cout << last_value << "\t" << new_value << std::endl;
  
  // do {
  //   status = s.write(verbose);
  // } while(status == RdKafka::ERR_NO_ERROR);
  // s.write(verbose);
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
