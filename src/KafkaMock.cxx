#include <KafkaMock.hpp>

namespace RdKafka {

  static Conf mock_configuration;
  static Consumer mock_consumer;
  static Message mock_message;
  static Topic mock_topic;
  
  Conf* Conf::create(const int&) { return &mock_configuration; }
  bool Conf::set(const std::string& name, const std::string& value, std::string& err) {
    err="no error";
    return CONF_OK;
  }

  bool ErrorCode::operator==(const ErrorCode& other) { return value==other.value;  }
  bool ErrorCode::operator!=(const ErrorCode& other) { return value!=other.value; }

  ErrorCode::ErrorCode(int n) : value(n) { };
  ErrorCode Message::err() { return ERR_NO_ERROR; }
  void* Message::payload() { return static_cast<void*>(&load); }
  
  Consumer* Consumer::create(Conf* configuration, std::string& err) {
    err="no error";
    return &mock_consumer;
  }
  ErrorCode Consumer::start(Topic* , const int& , const int&) { return ErrorCode(0); }
  int Consumer::stop(Topic*, const int&) { return 0; }
  Message* Consumer::consume(Topic*, const int&, const int&) {
    return &mock_message;
  }

  Topic* Topic::create(Consumer* consumer,
                       const std::string& name,
                       Conf* tconf,
                       std::string err) {
    err="no error";
    return &mock_topic;
  }
  
  std::string err2str(ErrorCode err) { return std::string("error number = ")+std::to_string(int(err)); }


  
}
