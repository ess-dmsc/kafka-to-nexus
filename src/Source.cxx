#include "Source.h"

namespace BrightnESS {
namespace FileWriter {

Source::Source(std::string topic, std::string source)
	:	_topic(topic),
		_source(source) {
}

Source::Source(Source && x) :
	_topic(std::move(x._topic)),
	_source(std::move(x._source))
{	
}

}
}
