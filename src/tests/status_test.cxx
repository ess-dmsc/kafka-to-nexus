#include <thread>
#include <random>
#include <type_traits>

#include <gtest/gtest.h>

#include <Status.hpp>

using StreamMasterErrorCode = FileWriter::Status::StreamMasterErrorCode;
using MessageInfo = FileWriter::Status::MessageInfo;
using StreamMasterInfo = FileWriter::Status::StreamMasterInfo;

constexpr int n_messages{10000};


TEST(MessageInfo, zero_initialisation) {
  MessageInfo mi;
  ASSERT_DOUBLE_EQ(mi.messages().first, 0.0);
  ASSERT_DOUBLE_EQ(mi.messages().second, 0.0);
  ASSERT_DOUBLE_EQ(mi.Mbytes().first, 0.0);
  ASSERT_DOUBLE_EQ(mi.Mbytes().second, 0.0);
  ASSERT_DOUBLE_EQ(mi.errors(), 0.0);
}

TEST(MessageInfo, add_one_message) {
  MessageInfo mi;
  const double new_message_bytes{1024};
  mi.message(new_message_bytes);

  EXPECT_DOUBLE_EQ(mi.messages().first, 1.0);
  EXPECT_DOUBLE_EQ(mi.messages().second, 1.0);
  EXPECT_DOUBLE_EQ(mi.Mbytes().first, new_message_bytes*1e-6);
  EXPECT_DOUBLE_EQ(mi.Mbytes().second,  new_message_bytes*new_message_bytes*1e-12);
  EXPECT_DOUBLE_EQ(mi.errors(), 0.0);
}

TEST(MessageInfo, add_one_error) {
  MessageInfo mi;
  mi.error();

  EXPECT_DOUBLE_EQ(mi.messages().first, 0.0);
  EXPECT_DOUBLE_EQ(mi.messages().second, 0.0);
  EXPECT_DOUBLE_EQ(mi.Mbytes().first, 0.0);
  EXPECT_DOUBLE_EQ(mi.Mbytes().second, 0.0);
  EXPECT_DOUBLE_EQ(mi.errors(), 1.0);
}

TEST(MessageInfo, add_messages) {
  MessageInfo mi;
  std::default_random_engine generator;
  std::normal_distribution<double> normal(0.0, 1.0);

  double accum{0.0}, accum2{0.0};
  for(int i=0;i<n_messages;++i) {
    auto new_message_bytes = std::fabs(normal(generator));
    accum  += new_message_bytes;
    accum2 += new_message_bytes*new_message_bytes;
    mi.message(new_message_bytes);
  }

  EXPECT_DOUBLE_EQ(mi.messages().first, n_messages);
  EXPECT_DOUBLE_EQ(mi.messages().second, n_messages);
  EXPECT_NEAR(mi.Mbytes().first, accum*1e-6, 1e-6 );
  EXPECT_NEAR(mi.Mbytes().second, accum2*1e-12, 1e-12);
  EXPECT_DOUBLE_EQ(mi.errors(), 0.0);
}

TEST(MessageInfo, reduce_message_info_empty) {

  std::vector<MessageInfo> vmi(10);  
  MessageInfo mi;
  for(auto& mit: vmi) {
    mi += mit;
  }

  EXPECT_DOUBLE_EQ(mi.messages().first, 0.0);
  EXPECT_DOUBLE_EQ(mi.messages().second, 0.0);
  EXPECT_NEAR(mi.Mbytes().first, 0.0, 1e-6 );
  EXPECT_NEAR(mi.Mbytes().second, 0.0, 1e-12);
  EXPECT_DOUBLE_EQ(mi.errors(), 0.0);
}

TEST(MessageInfo, reduce_message_info) {
  std::default_random_engine generator;
  std::normal_distribution<double> normal(0.0, 1.0);

  std::vector<MessageInfo> vmi(10);  

  double tot_msg{0.0}, tot_msg2{0.0};
  double tot_size{0.0}, tot_size2{0.0};
  for(auto& mit: vmi) {
    for(int i=0;i<10;++i) {
      mit.message(std::fabs(normal(generator)));
    }
    tot_msg   += mit.messages().first;
    tot_msg2  += mit.messages().second;
    tot_size  += mit.Mbytes().first;
    tot_size2 += mit.Mbytes().second;
  }

  MessageInfo mi;
  for(auto& mit: vmi) {
    mi += mit;
  }

  EXPECT_DOUBLE_EQ(mi.messages().first, tot_msg);
  EXPECT_DOUBLE_EQ(mi.messages().second, tot_msg2);
  EXPECT_NEAR(mi.Mbytes().first, tot_size, 1e-6 );
  EXPECT_NEAR(mi.Mbytes().second, tot_size2, 1e-12);
  EXPECT_DOUBLE_EQ(mi.errors(), 0.0);
}

TEST(MessageInfo, copy_message_info) {
  std::default_random_engine generator;
  std::normal_distribution<double> normal(0.0, 1.0);

  MessageInfo mi;
  for(int i=0;i<n_messages;++i) {
    mi.message(std::fabs(normal(generator)));
  }
  for(int i=0;i<10;++i) {
    mi.error();
  }
  MessageInfo new_mi;
  new_mi = mi;

  EXPECT_EQ(new_mi.messages().first, mi.messages().first);
  EXPECT_EQ(new_mi.messages().second, mi.messages().second);
  EXPECT_EQ(new_mi.Mbytes().first, mi.Mbytes().first);
  EXPECT_EQ(new_mi.Mbytes().second,mi.Mbytes().second);
  EXPECT_EQ(new_mi.errors(), mi.errors());
}


TEST(StreamMasterInfo, initialize_empty) {
  StreamMasterInfo info;
  auto& value = info.info(); 
  EXPECT_TRUE(value.size() == 0);
}

template<class T> using remove_const_t = typename std::remove_const<T>::type;

TEST(StreamMasterInfo, add_one_info) {
  std::default_random_engine generator;
  std::normal_distribution<double> normal(0.0, 1.0);

  StreamMasterInfo info;

  // other is required because MessageInfo::add() resets values
  MessageInfo mi,other;
  for(int i=0;i<n_messages;++i) {
    auto msg = std::fabs(normal(generator));
    mi.message(msg);
    other.message(msg);
  }
  for(int i=0;i<10;++i) {
    mi.error();
    other.error();
  }
  info.add("topic",mi);

  auto& value = info.info();
  EXPECT_TRUE(value.size() == 1);
  
  EXPECT_EQ(value["topic"].messages().first, other.messages().first);
  EXPECT_EQ(value["topic"].messages().second, other.messages().second);
  EXPECT_EQ(value["topic"].Mbytes().first, other.Mbytes().first);
  EXPECT_EQ(value["topic"].Mbytes().second,other.Mbytes().second);
  EXPECT_EQ(value["topic"].errors(), other.errors());
}

TEST(StreamMasterInfo, add_multiple_infos) {
  StreamMasterInfo info;
  const std::vector<std::string> topics{"first","second","third"};
  
  for(auto& t : topics) {
    MessageInfo mi;
    info.add(t,mi);
  }
  
  auto& value = info.info(); 
  EXPECT_TRUE(value.size() == topics.size());
}

TEST(StreamMasterInfo, add_accumulate_all_infos) {
  std::default_random_engine generator;
  std::normal_distribution<double> normal(0.0, 1.0);

  StreamMasterInfo info;
  const std::vector<std::string> topics{"first","second","third"};
  
  double tot_msg{0.0}, tot_msg2{0.0};
  double tot_size{0.0}, tot_size2{0.0};
  double tot_errors{0.0};

  for(auto& t : topics) {
    MessageInfo mi;
    for(int i=0;i<10;++i) {
      auto message_size = std::fabs(normal(generator)); 
      mi.message(message_size);

      tot_msg   += 1.0;
      tot_msg2  += 1.0;
      tot_size  += message_size*1e-6;
      tot_size2 += message_size*message_size*1e-12;
    }
    for(int i=0;i<10;++i) {
      mi.error();
      tot_errors += 1.0;
    }
    info.add(t,mi);
  }
  
  auto& value = info.total();   
  EXPECT_DOUBLE_EQ(value.messages().first, tot_msg);
  EXPECT_DOUBLE_EQ(value.messages().second, tot_msg2);
  EXPECT_NEAR(value.Mbytes().first, tot_size, 1e-6 );
  EXPECT_NEAR(value.Mbytes().second, tot_size2, 1e-12);
  EXPECT_DOUBLE_EQ(value.errors(), tot_errors);

}

TEST(MessageInfo, compute_derived_quantities) {
  const std::vector<double> messages_size{1.0,2.0,3.0,4.0,5.0};
  MessageInfo mi;
  
  for(auto& m : messages_size) {
    mi.message(m*1e6);
  }
  double duration{12.23};
  
  auto size = FileWriter::Status::message_size(mi);
  auto frequency = FileWriter::Status::message_frequency(mi,duration);
  auto throughput = FileWriter::Status::message_throughput(mi,duration);
  EXPECT_DOUBLE_EQ(size.first,3.0);
  EXPECT_NEAR(size.second,1.5811388300841898,10e-15); // unbiased
  EXPECT_NEAR(frequency.first,messages_size.size()/duration,10e-15);
  EXPECT_NEAR(throughput.first,std::accumulate(messages_size.begin(),messages_size.end(),0.0)/duration,10e-15);
}

TEST(MessageInfo, derived_quantities_null_divider) {
  const std::vector<double> messages_size{1.0,2.0,3.0,4.0,5.0};
  MessageInfo mi;
  
  for(auto& m : messages_size) {
    mi.message(m*1e6);
  }
  double duration{0};
  
  auto frequency = FileWriter::Status::message_frequency(mi,duration);
  auto throughput = FileWriter::Status::message_throughput(mi,duration);
  EXPECT_DOUBLE_EQ(0.0,frequency.first);
  EXPECT_DOUBLE_EQ(0.0,frequency.second);
  EXPECT_DOUBLE_EQ(0.0,throughput.first);
  EXPECT_DOUBLE_EQ(0.0,throughput.second);
}


