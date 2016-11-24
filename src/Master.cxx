#include "Master.h"
#include "NexusWriter.h"
#include "logger.h"
#include "helper.h"
#include <rapidjson/document.h>


namespace BrightnESS {
namespace FileWriter {

using std::vector;
using std::string;


/// Stub, will perform the JSON parsing and then take appropriate action.
class CommandHandler : public FileWriterCommandHandler {
public:
void handle(std::unique_ptr<CmdMsg> msg) {
	LOG(3, "CommandHandler got message: {}", (char*)msg->data());
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
