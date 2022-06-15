#include <regex>
#include <vector>
#include <string>
#include <thread>
#include <cstdio>
#include <chrono>
#include <errno.h>
#include <iostream>
#include <sstream>
#include <fstream>
#include <csignal>
#include <iomanip>
#include <getopt.h>
#include <unistd.h>
#include <limits.h>
#include <sys/inotify.h>
#include <grpcpp/grpcpp.h>

#include "dfslib-shared-p1.h"
#include "proto-src/dfs-service.grpc.pb.h"
#include "dfslib-clientnode-p1.h"
#include <google/protobuf/util/time_util.h>

using grpc::Status;
using grpc::Channel;
using grpc::StatusCode;
using grpc::ClientWriter;
using grpc::ClientReader;
using grpc::ClientContext;

//for deadline
using std::chrono::system_clock;
using std::chrono::milliseconds;
using std::chrono::time_point;
using namespace std;

using dfs_service::DFSService;
using dfs_service::FileRet;
using dfs_service::FileData;
using dfs_service::NoArg;
using dfs_service::File;
using dfs_service::FileList;
using dfs_service::FileStatus;
using google::protobuf::Timestamp;
using google::protobuf::util::TimeUtil;

//
// STUDENT INSTRUCTION:
//
// You may want to add aliases to your namespaced service methods here.
// All of the methods will be under the `dfs_service` namespace.
//
// For example, if you have a method named MyMethod, add
// the following:
//
//      using dfs_service::MyMethod
//


DFSClientNodeP1::DFSClientNodeP1() : DFSClientNode() {}

DFSClientNodeP1::~DFSClientNodeP1() noexcept {}

StatusCode DFSClientNodeP1::Store(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to store a file here. This method should
    // connect to your gRPC service implementation method
    // that can accept and store a file.
    //
    // When working with files in gRPC you'll need to stream
    // the file contents, so consider the use of gRPC's ClientWriter.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the client
    // StatusCode::CANCELLED otherwise
    //
    //

    //create context and FileRet to be passed to StoreFile
    ClientContext context;
    //match the file name metadata with what the server is expecting
    context.AddMetadata("file_name", filename);
    //set deadline
    system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
    context.set_deadline(deadline);

    //Valid path check
    const string &path = WrapPath(filename);
    //check status of the path
    struct stat file_stat;
    if (stat(path.c_str(), &file_stat) != 0) {
        const string &err = "Store path check fail. Path: ";
        dfs_log(LL_ERROR) << err << path;
        return StatusCode::NOT_FOUND;
    }
    
    FileRet ret;
    //Client writer
    unique_ptr<ClientWriter<FileData>> writer = service_stub->StoreFile(&context, &ret);
    
    ifstream infile(path);
    FileData data;

    int bytes_sent = 0;
    char buffer[DFS_BUFFERSIZE];
    int bytes_to_send = 0;
    int file_size = file_stat.st_size;
    while(bytes_sent < file_size) {
        if (file_size - bytes_sent < DFS_BUFFERSIZE) {
            bytes_to_send = file_size - bytes_sent;
        }
        else {
            bytes_to_send = DFS_BUFFERSIZE;
        }
        //read and set the contents for the FileData object
        infile.read(buffer, bytes_to_send);
        data.set_contents(buffer, bytes_to_send);
        //writer writes the data to the stream
        writer->Write(data);
        bytes_sent += bytes_to_send;
    }
    infile.close();

    writer->WritesDone();

    Status status = writer->Finish();

    if (!status.ok()) {
        const string &err = "Client store error ";
        dfs_log(LL_ERROR) << err << status.error_message();
        return status.error_code();
    }
    dfs_log(LL_SYSINFO) << "Client successfully stored: " << bytes_sent;
    return StatusCode::OK;
}


StatusCode DFSClientNodeP1::Fetch(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to fetch a file here. This method should
    // connect to your gRPC service implementation method
    // that can accept a file request and return the contents
    // of a file from the service.
    //
    // As with the store function, you'll need to stream the
    // contents, so consider the use of gRPC's ClientReader.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //
    //

    ClientContext context;
    File request;
    
    //set deadline and file name for FetchFile arguments
    system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
    context.set_deadline(deadline);

    request.set_name(filename);


    //Valid path check
    const string &path = WrapPath(filename);

    //client reader
    unique_ptr<ClientReader<FileData>> reader = service_stub->FetchFile(&context, request);

    ofstream ofs;
    ofs.open(path);

    if (!ofs.is_open()) {
        const string &err = "Error opening file with ofstream";
        dfs_log(LL_ERROR) << err;
        return StatusCode::INTERNAL;
    }
    

    //store the file from stream in FileData object. 
    //Track bytes read to ensure whole expected file is stored
    FileData data;
    while (reader->Read(&data)){
        //contents to string
        const string &data_chunk = data.contents();
        ofs << data_chunk;
    }
    ofs.close();

    Status status = reader->Finish();

    if (!status.ok()) {
        const string &err = "Client fetcg error ";
        dfs_log(LL_ERROR) << err << status.error_message();
        return status.error_code();
    }
    dfs_log(LL_SYSINFO) << "Client successfully fetched" ;
    return StatusCode::OK;
}

StatusCode DFSClientNodeP1::Delete(const std::string& filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to delete a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //
    //
    //create context and FileRet to be passed to DeleteFile
    ClientContext context;
    //set deadline
    system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
    context.set_deadline(deadline);
    
    //check status of the path
    struct stat file_stat;

    //Valid path check
    const string &path = WrapPath(filename);

    if (stat(path.c_str(), &file_stat) == 0) {
        //delete in client
        remove(path.c_str());
    }
    else {
        const string &err = "Delete path not found. Path: ";
        dfs_log(LL_SYSINFO) << err << path;
    }

    //delete on server
    File file;
    FileRet ret;
    file.set_name(filename);

    Status status = service_stub->DeleteFile(&context, file, &ret);

    if (!status.ok()) {
        const string &err = "Client delete error ";
        dfs_log(LL_ERROR) << err << status.error_message();
        return status.error_code();
    }
    dfs_log(LL_SYSINFO) << "Client successfully deleted: " << filename;
    return StatusCode::OK;


}


StatusCode DFSClientNodeP1::Stat(const std::string &filename, void* file_status) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to get the status of a file here. This method should
    // retrieve the status of a file on the server. Note that you won't be
    // tested on this method, but you will most likely find that you need
    // a way to get the status of a file in order to synchronize later.
    //
    // The status might include data such as name, size, mtime, crc, etc.
    //
    // The file_status is left as a void* so that you can use it to pass
    // a message type that you defined. For instance, may want to use that message
    // type after calling Stat from another method.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //
    //

    //create context and FileRet to be passed to Stat
    ClientContext context;
    //set deadline
    system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
    context.set_deadline(deadline);

    //create file
    FileStatus response;
    File request;
    

    request.set_name(filename);

    Status status = service_stub->GetStatus(&context, request, &response);

    if (!status.ok()) {
        const string &err = "Client Stat error ";
        dfs_log(LL_ERROR) << err << status.error_message();
        return status.error_code();
    }
    file_status = &response;
    dfs_log(LL_SYSINFO) << "Client Stat successful: ";
    return StatusCode::OK;
}

StatusCode DFSClientNodeP1::List(std::map<std::string,int>* file_map, bool display) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to list all files here. This method
    // should connect to your service's list method and return
    // a list of files using the message type you created.
    //
    // The file_map parameter is a simple map of files. You should fill
    // the file_map with the list of files you receive with keys as the
    // file name and values as the modified time (mtime) of the file
    // received from the server.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::CANCELLED otherwise
    //
    //
    ClientContext context;
    FileList response;
    NoArg request;
    
    //set deadline and file name for FetchFile arguments
    system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
    context.set_deadline(deadline);

    Status status = service_stub->ListFiles(&context, request, &response);
    
    // add file names and time modified into the file map
    for (const FileRet &ret : response.ret()) {
        int time_modified = TimeUtil::TimestampToSeconds(ret.time_modified());
        //insert into file map
        file_map->insert(pair<string, int>(ret.file_name(), time_modified));
        dfs_log(LL_SYSINFO) << "file added to map: " << ret.file_name();
    }

    if (!status.ok()) {
        const string &err = "Client File List error ";
        dfs_log(LL_ERROR) << err << status.error_message();
        return status.error_code();
    }
    dfs_log(LL_SYSINFO) << "Client File List successful: ";
    return StatusCode::OK;
}
//
// STUDENT INSTRUCTION:
//
// Add your additional code here, including
// implementations of your client methods
//

