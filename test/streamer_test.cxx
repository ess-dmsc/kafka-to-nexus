#include <gtest/gtest.h>
#include <algorithm>
#include <stdexcept>
#include <thread>

#include <Streamer.hpp>
#include <librdkafka/rdkafkacpp.h>

std::string broker("129.168.10.11:9092");
std::string topic("test");

const int max_recv_messages = 10;

using namespace BrightnESS::FileWriter;

std::function<ProcessMessageResult(void*,int)> silent = [](void* x, int) { return ProcessMessageResult::OK(); };

std::function<ProcessMessageResult(void*,int)> verbose = [](void* x, int size) {
  std::cout << "message: " << std::string((char*)x) << "\t" << size << std::endl;
  return ProcessMessageResult::OK();
};
std::function<ProcessMessageResult(void*,int,int*)> sum = [](void* x, int size, int* data_size) {
  (*data_size) += size;
  return ProcessMessageResult::OK();
};


std::function<TimeDifferenceFromMessage_DT(void*,int)> time_difference = [](void* x, int size) {
  std::cout << "message: " << std::string((char*)x) << "\t" << size << std::endl;
  return TimeDifferenceFromMessage_DT::OK();
};


TEST (Streamer, MissingTopicFailure) {
  ASSERT_THROW(Streamer(std::string("data_server:1234"),std::string("")),std::runtime_error);
}

TEST (Streamer, ConstructionSuccess) {
  ASSERT_NO_THROW(Streamer(broker,topic));
}

TEST (Streamer, NoReceive) {
  Streamer s(broker,"dummy_topic");
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


TEST (Streamer, Receive) {
  using namespace std::placeholders;

  Streamer s(broker,topic);
  int data_size=0,counter =0;
  std::function<ProcessMessageResult(void*,int)> f1 = std::bind (sum,_1,_2,&data_size); 
  
  ProcessMessageResult status = ProcessMessageResult::OK();
  status = s.write(silent);
  ASSERT_TRUE(status.is_OK());
  
  do {
    EXPECT_TRUE(status.is_OK());
    status = s.write(f1);
    ++counter;
  } while(status.is_OK()  && (counter < max_recv_messages ));
   EXPECT_GT(data_size,0);

}

TEST (Streamer, Reconnect) {
  using namespace std::placeholders;
    
  int data_size=0,counter =0;
  std::function<ProcessMessageResult(void*,int)> f1 = std::bind (sum,_1,_2,&data_size); 

  Streamer s(broker,"dummy_topic");
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

  EXPECT_EQ(s.connect(broker,topic),0);
  status = ProcessMessageResult::OK();
  do {
    EXPECT_TRUE(status.is_OK());
    status = s.write(f1);
    ++counter;
  } while(status.is_OK() && (counter < max_recv_messages ));
  EXPECT_TRUE(data_size > 0);  

}

TEST (Streamer, SearchBackward) {
  Streamer s(broker,topic);
  ProcessMessageResult status = ProcessMessageResult::OK();
  int counter=0;

  do {
    EXPECT_TRUE(status.is_OK());
    status = s.write(verbose);
    ++counter;
  } while(status.is_OK()  && (counter < max_recv_messages ));

  TimeDifferenceFromMessage_DT dt = s.search_backward(time_difference);
  std::cout << dt.sourcename << "\t" << dt.dt << "\n";

  do {
    ++counter;
    status = s.write(silent);
  } while(status.is_OK());
  EXPECT_EQ( counter, Streamer::step_back_amount);
  
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
