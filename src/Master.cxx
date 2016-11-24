#include "Master.h"
#include "NexusWriter.h"
#include "logger.h"
#include "helper.h"


namespace BrightnESS {
namespace FileWriter {

using std::vector;
using std::string;

Master::Master(MasterConfig config) :
config(config),
command_listener(config.command_listener)
{
}





}
}
