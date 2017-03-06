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
	  StrongType (T value) : m_value{value} { }
	  inline const T& value () { return m_value; }
	  bool operator==(const StrongType<T,PHANTOM>& other) {
	    return m_value == other.m_value;
	  }
	  bool operator!=(const StrongType<T,PHANTOM>& other) {
	    return m_value != other.m_value;
	  }
	  StrongType<T,PHANTOM>& operator+=(const StrongType<T,PHANTOM>& other) {
	    m_value -= other.m_value;
	    return *this;
	  }
	  StrongType<T,PHANTOM>& operator-=(const StrongType<T,PHANTOM>& other) {
	    m_value -= other.m_value;
	    return *this;
	  }
	private:
	  T m_value;

	};
    
      struct OffsetType {}; 
      struct PartitionType {};
      struct Timestamp {};

    }

    typedef utils::StrongType<int64_t,utils::OffsetType> RdKafkaOffset;
    typedef utils::StrongType<int32_t,utils::PartitionType> RdKafkaPartition;
    const RdKafkaOffset RdKafkaOffsetBegin(-2);
    const RdKafkaOffset RdKafkaOffsetEnd(-1);
    const RdKafkaOffset RdKafkaPartitionZero(-1);

    typedef nanoseconds ESSTimeStamp;

  }
}
