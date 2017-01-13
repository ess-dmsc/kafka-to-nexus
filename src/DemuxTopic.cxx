#include "DemuxTopic.h"

namespace BrightnESS {
namespace FileWriter {

DemuxTopic::DemuxTopic(std::string topic) : _topic(topic) {
}

DemuxTopic::DT DemuxTopic::time_difference_from_message(void * msg_data, int msg_size) {
	// TODO
	// a dummy so far
	static std::string _tmp_dummy;
	return DT(_tmp_dummy, 0);
}

}
}
