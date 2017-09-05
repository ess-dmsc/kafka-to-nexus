#include "logger.h"
#include <mpi.h>
#include <vector>

int main(int argc, char **argv) {
  MPI_Init(&argc, &argv);
  int rank, size;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);
  LOG(3, "mpi-worker as {} of {}", rank, size);
  std::vector<char> buf(128);
  MPI_Comm comm;
  MPI_Comm_get_parent(&comm);
  // or MPI_STATUS_IGNORE
  MPI_Status status;
  MPI_Recv(buf.data(), buf.size(), MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, comm,
           &status);
  int count;
  MPI_Get_count(&status, MPI_CHAR, &count);
  LOG(3, "status: {}, {}, {}, count: {}", status.MPI_SOURCE, status.MPI_TAG,
      status.MPI_ERROR, count);
  LOG(3, "received: {}", buf.data());
  MPI_Finalize();
  LOG(3, "mpi worker after finalize");
  return 42;
}
