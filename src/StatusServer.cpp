#include <StatusServer.h>
#include <inttypes.h>

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/wait.h>


void StatusServer::Start() {
    // This will start the thread. Notice move semantics!
    Thread = std::thread(&StatusServer::ThreadMain,this);
}


// get sockaddr, IPv4
void * get_in_addr(struct sockaddr *sa) {
    return sa->sa_family == AF_INET
    ? (void *) &(((struct sockaddr_in*)sa)->sin_addr)
    : (void *) &(((struct sockaddr_in6*)sa)->sin6_addr);
}

StatusServer::StatusServer() : Thread() {}

StatusServer::~StatusServer() {
    StopThread = true;
    if (Thread.joinable()) {
        Thread.join();
    }
}

void StatusServer::ThreadMain() {
    int sockfd, new_fd;  // listen on sock_fd, new connection on new_fd
    struct addrinfo hints, *servinfo, *p;
    struct sockaddr_storage their_addr; // connector's address information
    socklen_t sin_size;
    int yes=1;
    char s[INET6_ADDRSTRLEN];
    int rv;

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE; // use my IP

    if ((rv = getaddrinfo(NULL, tcp_port, &hints, &servinfo)) != 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
        return;
    }

    // loop through all the results and bind to the first we can
    for(p = servinfo; p != NULL; p = p->ai_next) {
        if ((sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1) {
            perror("server: socket");
            continue;
        }

        if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1) {
            perror("setsockopt");
            return;
        }

        if (bind(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
            close(sockfd);
            perror("server: bind");
            continue;
        }

        break;
    }

    freeaddrinfo(servinfo); // all done with this structure

    if (p == NULL)  {
        fprintf(stderr, "server: failed to bind\n");
        return;
    }

    if (listen(sockfd, Backlog) == -1) {
        perror("listen");
        return;
    }

    printf("server: waiting for connections...\n");

    while (!StopThread) { // main accept() loop
        sin_size = sizeof their_addr;
        new_fd = accept(sockfd, (struct sockaddr *)&their_addr, &sin_size);
        if (new_fd == -1) {
            perror("accept");
            continue;
        }
    // Check that we have free slot to handle this connection.
    // This aims to limit and prevent malfunctioning frequent connection request,
    // to exhaust system resources.
    if (active_threads.load() >= MAX_ACTIVE_THREADS) {
        // Too many active threads, reject connection
        close(new_fd);
        fprintf(stderr, "Too many active connections, rejecting\n");
        continue;
    }

    // Set socket send/receive timeouts (5 seconds) to prevent unfinished malfunctioning
    // connections consume resource and hanging forever.
    struct timeval timeout;
    timeout.tv_sec = 5;
    timeout.tv_usec = 0;
    setsockopt(new_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    setsockopt(new_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        inet_ntop(their_addr.ss_family, get_in_addr((struct sockaddr *)&their_addr), s, sizeof s);
        printf("server: got connection from %s\n", s);
// Register that we start a new con handler thread
active_threads++;
        std::thread([new_fd]() { // thread to handle the connection
            char buffer[1024];

            uint64_t global_state = GlobalState.load();
            uint64_t writes_done = GlobalWritesDone.load();
            int len = snprintf(buffer, 1024, "State %" PRIu64 ", WritesDone %" PRIu64 "\n", global_state, writes_done);
            if (send(new_fd, buffer, len, 0) == -1)
                perror("send");
            close(new_fd);
            // De-register con handler thread, since it's finished.
            active_threads--;
        }).detach();
    }
    return;
}

