#include <gtest/gtest.h>
#include <algorithm>
#include <stdexcept>

#include <Streamer.hpp>

std::string broker("129.168.10.11:9092");
std::string topic("test");

struct MockConsumer {
  virtual void process_data(void* data) {
    data_size = sizeof(*static_cast<int64_t*>(data));
  }
  int data_size=0;
};


TEST (Streamer, MissingTopicFailure) {
  ASSERT_THROW(Streamer(std::string(""),std::string("data_server:1234")),std::runtime_error);
}

TEST (Streamer, ConstructionSuccess) {
  ASSERT_NO_THROW(Streamer(topic,broker));
}

TEST (Streamer, NoReceive) {
  Streamer s(topic+"_no",broker);
  int f;
  EXPECT_FALSE( s.recv(f) ) ;
  std::function<void(void*)> f1 = [](void*) { std::cout << "hello!" << std::endl; };
  EXPECT_TRUE( s.recv(f1) );
  EXPECT_TRUE( s.len() == 0 );
}


// TEST (Streamer, Reconnect) {
//   Streamer s(topic,broker);
//   EXPECT_EQ(s.disconnect(),0);
//   EXPECT_EQ(s.connect(topic,broker),0);
// }



TEST (Streamer, Receive) {
  Streamer s(topic,broker);
  
  // int data_size = 0;
  // std::function<void(void*)> f1 = [&](void*) { std::cout << "hello!" << std::endl; data_size++; return; };
  // ASSERT_NO_THROW( s.recv(f1) );
  // EXPECT_GT(data_size,0);
  
  MockConsumer m;
  int counter = 0;
  std::function<void(void*)> f = std::bind(&MockConsumer::process_data,&m, std::placeholders::_1);
  while(s.recv(f) && (counter<10) ) {
    EXPECT_GT(m.data_size,0);
    ++counter;
  }
  std::cout << "num messages received: " << counter << std::endl;
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
