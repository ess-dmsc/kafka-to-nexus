#pragma once

#include <cstdint>
#include <thread>
#include <atomic>

// warning global variables
std::atomic<uint64_t> GlobalState;
std::atomic<uint64_t> GlobalWritesDone;
// Helpers to prevent resource exhaustion due to slow or malicious clients
constexpr int MAX_ACTIVE_THREADS = 100; // limit con handler threads
std::atomic<int> active_threads{0};     // count number of active connection handling.


const char * tcp_port = "3490";
const int Backlog{4};

class StatusServer {
public:
    StatusServer();
    ~StatusServer();
    
    void Start();

private:
    std::thread Thread;
    bool StopThread{false};
    void ThreadMain();

};
