#include "Source.h"

namespace BrightnESS {
namespace FileWriter {

Source::Source(std::string topic, std::string source)
	:	_topic(topic),
		_source(source) {
}

int64_t Source::time_difference_from_message(void * msg_data, int msg_size) {
	// TODO
	// a dummy so far
	return 0;
}

}
}
