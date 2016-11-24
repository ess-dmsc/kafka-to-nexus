#include "Master.h"
#include "NexusWriter.h"
#include "logger.h"
#include "helper.h"
#include <rapidjson/document.h>


namespace BrightnESS {
namespace FileWriter {

using std::vector;
using std::string;


class CommandHandler : public FileWriterCommandHandler {
public:
void handle(std::unique_ptr<CmdMsg> msg) {
}
};



Master::Master(MasterConfig config) :
	config(config),
	command_listener(config.command_listener)
{
	if (config.test_mockup_command_listener) {
		command_listener.is_mockup = true;
	}
}





}
}
