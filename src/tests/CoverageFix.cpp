#include <Master.h>
#include <kafka_util.h>
#include <CommandListener.h>
#include <ctime>

int DoNotCall() {
  MainOpt SomeOption;
  FileWriter::Master Test(SomeOption);
  if (time(nullptr)) {
    throw FileWriter::BrokerFailure("Dummy");
  }
  return 0;
}
