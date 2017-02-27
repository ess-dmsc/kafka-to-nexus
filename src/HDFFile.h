#pragma once
#include <memory>
#include <vector>
#include <string>
// we only need hid_t from this:
#include <hdf5.h>


namespace BrightnESS {
namespace FileWriter {

// Basically POD
class WriteResult {
public:
int64_t ts;
};


struct Msg {
char * data;
int32_t size;
};


class HDFFile_h5;
class HDFFile_impl;

class HDFFile final {
public:
HDFFile();
~HDFFile();
int init(std::string filename);
void flush();
HDFFile_h5 h5file_detail();
private:
std::unique_ptr<HDFFile_impl> impl;
};


class FBSchemaWriter;

class FBSchemaReader {
public:
typedef std::unique_ptr<FBSchemaReader> ptr;
static std::unique_ptr<FBSchemaReader> create(Msg msg);
virtual ~FBSchemaReader();
std::unique_ptr<FBSchemaWriter> create_writer();
std::string sourcename(Msg msg);
uint64_t ts(Msg msg);
uint64_t teamid(Msg & msg);
private:
virtual std::unique_ptr<FBSchemaWriter> create_writer_impl() = 0;
virtual std::string sourcename_impl(Msg msg) = 0;
virtual uint64_t ts_impl(Msg msg) = 0;
virtual uint64_t teamid_impl(Msg & msg);
};


class FBSchemaWriter {
public:
typedef std::unique_ptr<FBSchemaWriter> ptr;
virtual ~FBSchemaWriter();
void init(HDFFile * hdf_file, std::string const & sourcename, Msg msg);
WriteResult write(Msg msg);
void flush();
void close();
protected:
FBSchemaWriter();
HDFFile * hdf_file = nullptr;
private:
virtual void init_impl(std::string const & sourcename, hid_t hdf_group, Msg msg) = 0;
virtual WriteResult write_impl(Msg msg) = 0;
};


}
}
