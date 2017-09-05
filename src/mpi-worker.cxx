#include "logger.h"
#include <mpi.h>
#include <vector>

int main(int argc, char **argv) {
  int err = MPI_SUCCESS;
  MPI_Init(&argc, &argv);

  MPI_Comm MPI_COMM_NODE;
  MPI_Comm_split_type(MPI_COMM_WORLD, MPI_COMM_TYPE_SHARED, 0 /* key */,
                      MPI_INFO_NULL, &MPI_COMM_NODE);

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

  LOG(3, "wait for pointer");
  void *shm_ptr = nullptr;
  MPI_Recv(&shm_ptr, 8, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, comm, &status);
  // LOG(3, "read shared data: {}", shm_ptr);
  LOG(3, "recv shm_ptr: {}", (void *)shm_ptr);

  {
    int n = 0;
    MPI_Comm_size(comm, &n);
    LOG(3, "size of spawned: {}", n);
  }

  {
    int n = 0;
    MPI_Comm_size(MPI_COMM_NODE, &n);
    LOG(3, "size of MPI_COMM_NODE: {}", n);
  }

  MPI_Comm shm_comm = MPI_COMM_WORLD; // or MPI_COMM_NODE
  // TODO
  // Share the setup code with the main process
  size_t shm_size = 1 * 1024 * 1024;
  MPI_Win mpi_win;
  MPI_Info win_info;
  MPI_Info_create(&win_info);
  // MPI_Info_set(win_info, "alloc_shared_noncontig", "true");
  LOG(3, "MPI_Win_allocate_shared {}", rank);
  err = MPI_Win_allocate_shared(shm_size, 1, win_info, shm_comm, &shm_ptr,
                                &mpi_win);
  if (err != MPI_SUCCESS) {
    LOG(3, "failed MPI_Win_allocate_shared");
    exit(1);
  }
  LOG(3, "MPI_Win_allocate_shared {} DONE", rank);
  MPI_Info_free(&win_info);

  MPI_Win_lock_all(0, mpi_win);
  MPI_Win_sync(mpi_win);
  MPI_Barrier(comm);

  MPI_Comm_disconnect(&comm);
  LOG(3, "finalizing {}", rank);
  MPI_Finalize();
  LOG(3, "after finalize {}", rank);
  return 42;
}
