#include <regex>
#include <mutex>
#include <vector>
#include <string>
#include <thread>
#include <cstdio>
#include <chrono>
#include <errno.h>
#include <csignal>
#include <iostream>
#include <sstream>
#include <fstream>
#include <iomanip>
#include <getopt.h>
#include <unistd.h>
#include <limits.h>
#include <sys/inotify.h>
#include <grpcpp/grpcpp.h>
#include <utime.h>

#include "src/dfs-utils.h"
#include "src/dfslibx-clientnode-p2.h"
#include "dfslib-shared-p2.h"
#include "dfslib-clientnode-p2.h"
#include "proto-src/dfs-service.grpc.pb.h"
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

extern dfs_log_level_e DFS_LOG_LEVEL;
mutex directory_mutex;
//
// STUDENT INSTRUCTION:
//
// Change these "using" aliases to the specific
// message types you are using to indicate
// a file request and a listing of files from the server.
//
using FileRequestType = File;
using FileListResponseType = FileList;

DFSClientNodeP2::DFSClientNodeP2() : DFSClientNode() {}
DFSClientNodeP2::~DFSClientNodeP2() {}


grpc::StatusCode DFSClientNodeP2::Store(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to store a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation. However, you will
    // need to adjust this method to recognize when a file trying to be
    // stored is the same on the server (i.e. the ALREADY_EXISTS gRPC response).
    //
    // You will also need to add a request for a write lock before attempting to store.
    //
    // If the write lock request fails, you should return a status of RESOURCE_EXHAUSTED
    // and cancel the current operation.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::ALREADY_EXISTS - if the local cached file has not changed from the server version
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //
    ClientContext context;
    //match the file name metadata with what the server is expecting
    context.AddMetadata("client_id", ClientId());
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
    
    //acquire write lock
    StatusCode lockStat = RequestWriteAccess(filename);
    if (lockStat != StatusCode::OK) {
        const string &err = "Store write lock error ";
        dfs_log(LL_ERROR) << err;
        return StatusCode::RESOURCE_EXHAUSTED;
    }

    context.AddMetadata("crc", to_string(dfs_file_checksum(path, &crc_table)));


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


grpc::StatusCode DFSClientNodeP2::RequestWriteAccess(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to obtain a write lock here when trying to store a file.
    // This method should request a write lock for the given file at the server,
    // so that the current client becomes the sole creator/writer. If the server
    // responds with a RESOURCE_EXHAUSTED response, the client should cancel
    // the current file storage
    //
    // The StatusCode response should be:
    //
    // OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //
    ClientContext context;
    //add clietn ID for part 2
    context.AddMetadata("client_id", ClientId());

    FileRet response;
    File request;
    request.set_name(filename);

    Status status = service_stub->WriteLock(&context, request, &response);

    if (!status.ok()) {
        const string &err = "Client write lock error ";
        dfs_log(LL_ERROR) << err << status.error_message();
        return status.error_code();
    }
    dfs_log(LL_SYSINFO) << "Client write lock success: ";
    return StatusCode::OK;

}

grpc::StatusCode DFSClientNodeP2::Fetch(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to fetch a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation. However, you will
    // need to adjust this method to recognize when a file trying to be
    // fetched is the same on the client (i.e. the files do not differ
    // between the client and server and a fetch would be unnecessary.
    //
    // The StatusCode response should be:
    //
    // OK - if all went well
    // DEADLINE_EXCEEDED - if the deadline timeout occurs
    // NOT_FOUND - if the file cannot be found on the server
    // ALREADY_EXISTS - if the local cached file has not changed from the server version
    // CANCELLED otherwise
    //
    // Hint: You may want to match the mtime on local files to the server's mtime
    //

    ClientContext context;
    File request;
    
    //set deadline and file name for FetchFile arguments
    system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
    context.set_deadline(deadline);

    request.set_name(filename);


    //Valid path check
    const string &path = WrapPath(filename);

    //check status of the path, set modify parameters if needed
    struct stat file_stat;
    if (stat(path.c_str(), &file_stat) == 0) {
        const string &err = "File already exists, modifying";
        context.AddMetadata("time_modified",to_string(file_stat.st_mtime));
        return StatusCode::ALREADY_EXISTS;
    }
    context.AddMetadata("crc", to_string(dfs_file_checksum(path, &crc_table)));
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

grpc::StatusCode DFSClientNodeP2::Delete(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to delete a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You will also need to add a request for a write lock before attempting to delete.
    //
    // If the write lock request fails, you should return a status of RESOURCE_EXHAUSTED
    // and cancel the current operation.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //
    ClientContext context;
    //set deadline
    system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
    context.set_deadline(deadline);
    context.AddMetadata("client_id", ClientId());

    //acquire write lock
    StatusCode lockStat = RequestWriteAccess(filename);
    if (lockStat != StatusCode::OK) {
        const string &err = "Store write lock error ";
        dfs_log(LL_ERROR) << err;
        return StatusCode::RESOURCE_EXHAUSTED;
    }

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

grpc::StatusCode DFSClientNodeP2::List(std::map<std::string,int>* file_map, bool display) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to list files here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation and add any additional
    // listing details that would be useful to your solution to the list response.
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
    for (const FileStatus &ret : response.file_status()) {
        int time_modified = TimeUtil::TimestampToSeconds(ret.time_modified());
        //insert into file map
        file_map->insert(pair<string, int>(ret.name(), time_modified));
        dfs_log(LL_SYSINFO) << "file added to map: " << ret.name();
    }

    if (!status.ok()) {
        const string &err = "Client File List error ";
        dfs_log(LL_ERROR) << err << status.error_message();
        return status.error_code();
    }
    dfs_log(LL_SYSINFO) << "Client File List successful: ";
    return StatusCode::OK;
}

grpc::StatusCode DFSClientNodeP2::Stat(const std::string &filename, void* file_status) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to get the status of a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation and add any additional
    // status details that would be useful to your solution.
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

void DFSClientNodeP2::InotifyWatcherCallback(std::function<void()> callback) {

    //
    // STUDENT INSTRUCTION:
    //
    // This method gets called each time inotify signals a change
    // to a file on the file system. That is every time a file is
    // modified or created.
    //
    // You may want to consider how this section will affect
    // concurrent actions between the inotify watcher and the
    // asynchronous callbacks associated with the server.
    //
    // The callback method shown must be called here, but you may surround it with
    // whatever structures you feel are necessary to ensure proper coordination
    // between the async and watcher threads.
    //
    // Hint: how can you prevent race conditions between this thread and
    // the async thread when a file event has been signaled?
    //

    //lock mount mutex
    lock_guard<mutex> lock(directory_mutex);
    callback();

}

//
// STUDENT INSTRUCTION:
//
// This method handles the gRPC asynchronous callbacks from the server.
// We've provided the base structure for you, but you should review
// the hints provided in the STUDENT INSTRUCTION sections below
// in order to complete this method.
//
void DFSClientNodeP2::HandleCallbackList() {

    void* tag;

    bool ok = false;

    //
    // STUDENT INSTRUCTION:
    //
    // Add your file list synchronization code here.
    //
    // When the server responds to an asynchronous request for the CallbackList,
    // this method is called. You should then synchronize the
    // files between the server and the client based on the goals
    // described in the readme.
    //
    // In addition to synchronizing the files, you'll also need to ensure
    // that the async thread and the file watcher thread are cooperating. These
    // two threads could easily get into a race condition where both are trying
    // to write or fetch over top of each other. So, you'll need to determine
    // what type of locking/guarding is necessary to ensure the threads are
    // properly coordinated.
    //

    // Block until the next result is available in the completion queue.
    while (completion_queue.Next(&tag, &ok)) {
        {
            //
            // STUDENT INSTRUCTION:
            //
            // Consider adding a critical section or RAII style lock here
            //
            
            // The tag is the memory location of the call_data object
            AsyncClientData<FileListResponseType> *call_data = static_cast<AsyncClientData<FileListResponseType> *>(tag);

            dfs_log(LL_DEBUG2) << "Received completion queue callback";

            // Verify that the request was completed successfully. Note that "ok"
            // corresponds solely to the request for updates introduced by Finish().
            // GPR_ASSERT(ok);
            if (!ok) {
                dfs_log(LL_ERROR) << "Completion queue callback not ok.";
            }

            if (ok && call_data->status.ok()) {

                dfs_log(LL_DEBUG3) << "Handling async callback ";

                //
                // STUDENT INSTRUCTION:
                //
                // Add your handling of the asynchronous event calls here.
                // For example, based on the file listing returned from the server,
                // how should the client respond to this updated information?
                // Should it retrieve an updated version of the file?
                // Send an update to the server?
                // Do nothing?
                //
                lock_guard<mutex> lock(directory_mutex);

                for (const FileStatus &serverFS : call_data->reply.file_status()) {
                    //check status of the path
                    struct stat file_stat;
                    FileStatus client_FS;
                    //Valid path check
                    const string &path = WrapPath(serverFS.name());
                    StatusCode status;
                    if (stat(path.c_str(), &file_stat) != 0) {
                        //file doesn't exist in client cache, need to fetch
                        status = this->Fetch(serverFS.name());
                        if (status != StatusCode::OK) {
                            dfs_log(LL_ERROR) << "Fetch file error in HandleCallbackList";
                        }
                    }
                    else {
                        //client file exists so we retrieve the stats
                        Timestamp *time_modified = new Timestamp(TimeUtil::TimeTToTimestamp(file_stat.st_mtime));
                        Timestamp *time_created = new Timestamp(TimeUtil::TimeTToTimestamp(file_stat.st_ctime));
                        client_FS.set_allocated_time_modified(time_modified);
                        client_FS.set_allocated_time_created(time_created);
                        client_FS.set_name(serverFS.name());
                        client_FS.set_size(file_stat.st_size);

                        if(client_FS.time_modified() > serverFS.time_modified()) {
                            dfs_log(LL_SYSINFO)<< "server file out of date, storing client file";
                            status = this->Store(client_FS.name());
                            if (status != StatusCode::OK) {
                                dfs_log(LL_ERROR) << "Server file update failed";
                            }
                        }
                        else{
                            dfs_log(LL_SYSINFO)<< "client file out of date, fetch server file";
                            this->Fetch(serverFS.name());
                        }
                    }
                }


            } else {
                dfs_log(LL_ERROR) << "Status was not ok. Will try again in " << DFS_RESET_TIMEOUT << " milliseconds.";
                dfs_log(LL_ERROR) << call_data->status.error_message();
                std::this_thread::sleep_for(std::chrono::milliseconds(DFS_RESET_TIMEOUT));
            }

            // Once we're complete, deallocate the call_data object.
            delete call_data;

            //
            // STUDENT INSTRUCTION:
            //
            // Add any additional syncing/locking mechanisms you may need here

        }


        // Start the process over and wait for the next callback response
        dfs_log(LL_DEBUG3) << "Calling InitCallbackList";
        InitCallbackList();

    }
}

/**
 * This method will start the callback request to the server, requesting
 * an update whenever the server sees that files have been modified.
 *
 * We're making use of a template function here, so that we can keep some
 * of the more intricate workings of the async process out of the way, and
 * give you a chance to focus more on the project's requirements.
 */
void DFSClientNodeP2::InitCallbackList() {
    CallbackList<FileRequestType, FileListResponseType>();
}

//
// STUDENT INSTRUCTION:
//
// Add any additional code you need to here
//


