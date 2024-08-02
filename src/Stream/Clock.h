
#pragma once

#include <chrono>

class Clock {
public:
  using time_point = std::chrono::system_clock::time_point;
  using duration = std::chrono::system_clock::duration;

  virtual ~Clock() = default;

  [[nodiscard]] virtual time_point get_current_time() const = 0;
};

class SystemClock : public Clock {
public:
  [[nodiscard]] time_point get_current_time() const override {
    return std::chrono::system_clock::now();
  }
};

class FakeClock : public Clock {
public:
  void set_time(time_point time) { fake_time_point_ = time; }

  [[nodiscard]] time_point get_current_time() const override {
    return fake_time_point_;
  }

private:
  time_point fake_time_point_;
};