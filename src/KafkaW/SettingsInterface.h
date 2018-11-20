#pragma once

#include <map>
#include <string>
namespace KafkaW {
struct SettingsInterface {
  virtual void apply() = 0;
  std::map<std::string, std::string> Configuration;
};
}
