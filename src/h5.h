#pragma once
#include <array>
#include <vector>
#include <string>
#include <hdf5.h>

namespace h5 {

using std::array;
using std::vector;
using std::string;

namespace h5p {

class dataset_create {
public:
static dataset_create chunked1(hid_t type, hsize_t bytes);
dataset_create(dataset_create && x);
~dataset_create();
friend void swap(dataset_create & x, dataset_create & y);
hid_t id = -1;
private:
dataset_create();
dataset_create(dataset_create const & x);
};

}

class h5d;

class h5s {
public:
template <size_t N>
static h5s simple_unlim(array<hsize_t,N> const & sini);
h5s(h5d const & x);
h5s(h5s && x);
~h5s();
friend void swap(h5s & x, h5s & y);
hid_t id = -1;
private:
h5s();
};

struct append_ret {
int status;
uint64_t written_bytes;
uint64_t ix0;
operator bool () const { return status == 0; }
};

class h5d {
public:
static h5d create(hid_t loc, string name, hid_t type, h5s dsp, h5p::dataset_create dcpl);
h5d(hid_t loc, string name, hid_t type, h5s dsp, h5p::dataset_create dcpl);
template <typename T>
h5d(hid_t loc, string name, hsize_t chunk_bytes, T dummy = 0);
h5d(h5d && x);
~h5d();
friend void swap(h5d & x, h5d & y);
template <typename T>
append_ret append_data_1d(T const * data, hsize_t nlen);
hid_t id = -1;
hid_t type = -1;
private:
h5d();
};

template <typename T>
class h5d_chunked_1d {
public:
h5d_chunked_1d(hid_t loc, string name, hsize_t chunk_bytes);
h5d ds;
h5d_chunked_1d(h5d_chunked_1d && x);
~h5d_chunked_1d();
friend void swap(h5d_chunked_1d & x, h5d_chunked_1d & y);
append_ret append_data_1d(T const * data, hsize_t nlen);
int flush_buf();
private:
h5d_chunked_1d();
h5s dsp_wr;
std::vector<T> buf;
hsize_t i0 = 0;
};

}
