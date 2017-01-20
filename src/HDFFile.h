#pragma once
#include <memory>
#include <vector>
#include <string>


namespace BrightnESS {
namespace FileWriter {

class FlatBufferMsg {
public:
std::string src();
uint64_t ts();
private:
virtual std::string src_impl() = 0;
virtual uint64_t ts_impl() = 0;
};


class HDFFile_h5;
class HDFFile_impl;

class HDFFile final {
public:
HDFFile();
~HDFFile();
void init(std::string filename);
HDFFile_h5 h5file_detail();
private:
std::unique_ptr<HDFFile_impl> impl;
};


class FBSchemaWriter;

class FBSchemaReader {
public:
static std::unique_ptr<FBSchemaReader> create(char * msg_data, int msg_size);
std::unique_ptr<FBSchemaWriter> create_writer();
std::string sourcename(char * msg_data);
uint64_t ts(char * msg_data);
private:
virtual std::unique_ptr<FBSchemaWriter> create_writer_impl() = 0;
virtual std::string sourcename_impl(char * msg_data) = 0;
virtual uint64_t ts_impl(char * msg_data) = 0;
};


class FBSchemaWriter {
public:
~FBSchemaWriter();
void init(HDFFile & hdf_file, char * msg_data);
void write(char * msg_data);
void flush();
void close();
protected:
FBSchemaWriter();
private:
virtual void init_impl(HDFFile & hdf_file, char * msg_data) = 0;
virtual void write_impl(char * msg_data) = 0;
};


}
}
