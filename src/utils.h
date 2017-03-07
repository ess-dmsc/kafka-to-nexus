#pragma once

#include <chrono>

using milliseconds = std::chrono::milliseconds;
using microseconds = std::chrono::microseconds;
using nanoseconds = std::chrono::nanoseconds;
/* constexpr milliseconds operator "" _ms(const uint32_t value) { */
/*   return milliseconds(value); */
/* } */
/* constexpr microseconds operator "" _us(const uint64_t value) { */
/*   return microseconds(value); */
/* } */
/* constexpr nanoseconds operator "" _ns(const uint64_t value) { */
/*   return nanoseconds(value); */
/* } */


namespace BrightnESS {
  namespace FileWriter {
    namespace utils {
      
      template <typename T, typename PHANTOM>
	struct StrongType {
	public:
	  StrongType () { }
	  StrongType (T value) : m_value{value} { }
	  inline const T& value () { return m_value; }
	private:
	  T m_value;
	};
      
      struct OffsetType {}; 
      struct PartitionType {};
      struct Timestamp {};
      
    }
    
    /* typedef utils::StrongType<int64_t,utils::OffsetType> RdKafkaOffset; */
    /* typedef utils::StrongType<int32_t,utils::PartitionType> RdKafkaPartition; */
    using RdKafkaOffset=utils::StrongType<int64_t,utils::OffsetType>;
    using RdKafkaPartition=utils::StrongType<int32_t,utils::PartitionType>;

    const RdKafkaOffset RdKafkaOffsetEnd(-1);
    const RdKafkaOffset RdKafkaOffsetBegin(-2);
    typedef nanoseconds ESSTimeStamp;

  }
}
