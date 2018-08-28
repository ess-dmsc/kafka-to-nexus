#include <CommandListener.h>
#include <Master.h>
#include <ctime>
#include <kafka_util.h>

int DoNotCall() {
  MainOpt SomeOption;
  FileWriter::Master Test(SomeOption);
  if (time(nullptr)) {
    throw FileWriter::BrokerFailure("Dummy");
  }
  return 0;
}
