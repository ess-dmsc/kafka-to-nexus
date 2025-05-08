
#include <cstdint>
#include <thread>

// warning global variables
uint64_t GlobalWritesDone{0};
uint64_t GlobalState{0};


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