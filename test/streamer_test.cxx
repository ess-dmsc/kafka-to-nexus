#include <gtest/gtest.h>
#include <algorithm>
#include <stdexcept>

#include <Streamer.hpp>
#include <librdkafka/rdkafkacpp.h>

std::string broker("129.168.10.11:9092");
std::string topic("test");

struct MockConsumer {
  virtual void process_data(void* data) {
    data_size = sizeof(*static_cast<int64_t*>(data));
  }
  int data_size=0;
};


TEST (Streamer, MissingTopicFailure) {
  ASSERT_THROW(Streamer(std::string("data_server:1234")),std::string(""),std::runtime_error);
}

TEST (Streamer, ConstructionSuccess) {
  ASSERT_NO_THROW(Streamer(broker,topic));
}

TEST (Streamer, NoReceive) {
  Streamer s(broker,topic+"_no");
  int f;
  EXPECT_FALSE( s.write(f) == RdKafka::ERR_NO_ERROR ) ;
  auto f1 = [](void*,int) { std::cout << "hello!" << std::endl; };
  EXPECT_FALSE( s.write(f1) == RdKafka::ERR_NO_ERROR );
  EXPECT_TRUE( s.len() == 0 );
}


TEST (Streamer, Receive) {
  Streamer s(broker,topic);
  
  std::function<void(void*,int)> f1 = [](void* x, int size) { std::cout << std::string((char*)x) << std::endl; return; };

  int status = RdKafka::ERR_NO_ERROR;
  do {
    EXPECT_TRUE(status == RdKafka::ERR_NO_ERROR);
    status = s.write(f1);
  } while(status == RdKafka::ERR_NO_ERROR);
  // EXPECT_GT(data_size,0);
  
  // MockConsumer m;
  // int counter = 0;
  // //  std::function<void(void*)> f = std::bind(&MockConsumer::process_data,&m, std::placeholders::_1);
  
  // while(s.write(f) && (counter<10) ) {
  //   EXPECT_GT(m.data_size,0);
  //   ++counter;
  // }
  // std::cout << "num messages received: " << counter << std::endl;
}

// TEST (Streamer, Reconnect) {

//   std::function<void(void*)> f = [&](void* x) { return; };
//   std::function<void(void*)> f1 = [&](void* x) { std::cout << std::string((char*)x) << std::endl; return; };
  
//   Streamer (broker,topic+"_no");
//   EXPECT_EQ(s.disconnect(),0);
//   EXPECT_EQ(s.connect(broker,topic),0);
//   s.search_backward(f);
//   s.write(f1);
// }


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
