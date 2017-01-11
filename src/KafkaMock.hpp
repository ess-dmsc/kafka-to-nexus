#include <iostream>
#include <vector>
#include <string>


namespace RdKafka {

  struct Conf {
    enum { CONF_OK, CONF_TOPIC, CONF_GLOBAL };
    static Conf* create(const int&);
    bool set(const std::string&, const std::string&, std::string&);
  };

  struct ErrorCode {
    int value;
    ErrorCode(int);
    operator int() { return value; }
    bool operator==(const ErrorCode&);
    bool operator!=(const ErrorCode&);
  };
  
  struct Message {
    int load=1;
    ErrorCode err();
    size_t len() { return 1; }
    void* payload();
  };

  struct Topic;
  struct Consumer {
    static Consumer* create(Conf*, std::string&);
    ErrorCode start(Topic* , const int& , const int&);
    int stop(Topic*, const int&);
    Message* consume(Topic*, const int&, const int&);
  };

  struct Topic {
    enum { OFFSET_BEGINNING=0 };
    static Topic* create(Consumer*, const std::string&, Conf*,std::string);
  };

  std::string err2str(ErrorCode);

  static ErrorCode ERR_NO_ERROR(0);
  static ErrorCode ERR__PARTITION_EOF(1);

}
