#pragma once

#include <chrono>

using milliseconds = std::chrono::milliseconds;
constexpr milliseconds operator "" _ms(const unsigned long long int value) {
  return milliseconds(value);
}

typedef int64_t ESSTimeStamp;



namespace BrightnESS {
  namespace FileWriter {
    namespace utils {

      template <typename T, typename PHANTOM>
	struct StrongType {
	public:
	  explicit StrongType (T value) : m_value{value} { }
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
    }

    typedef utils::StrongType<int64_t,utils::OffsetType> RdKafkaOffset;
    typedef utils::StrongType<int32_t,utils::PartitionType> RdKafkaPartition;
  }
}
