#include <map>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <iostream>
#include <fstream>
#include <errno.h>
#include <dirent.h>
#include <sys/stat.h>
#include <getopt.h>
#include <grpcpp/grpcpp.h>

#include "src/dfs-utils.h"
#include "dfslib-servernode-p1.h"
#include "proto-src/dfs-service.grpc.pb.h"
#include "dfslib-shared-p1.h"
#include <google/protobuf/util/time_util.h>

using namespace std;
using grpc::Status;
using grpc::Server;
using grpc::StatusCode;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::ServerContext;
using grpc::ServerBuilder;

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
// DFSServiceImpl is the implementation service for the rpc methods
// and message types you defined in the `dfs-service.proto` file.
//
// You should add your definition overrides here for the specific
// methods that you created for your GRPC service protocol. The
// gRPC tutorial described in the readme is a good place to get started
// when trying to understand how to implement this class.
//
// The method signatures generated can be found in `proto-src/dfs-service.grpc.pb.h` file.
//
// Look for the following section:
//
//      class Service : public ::grpc::Service {
//
// The methods returning grpc::Status are the methods you'll want to override.
//
// In C++, you'll want to use the `override` directive as well. For example,
// if you have a service method named MyMethod that takes a MyMessageType
// and a ServerWriter, you'll want to override it similar to the following:
//
//      Status MyMethod(ServerContext* context,
//                      const MyMessageType* request,
//                      ServerWriter<MySegmentType> *writer) override {
//
//          /** code implementation here **/
//      }
//
class DFSServiceImpl final : public DFSService::Service {

private:

    /** The mount path for the server **/
    std::string mount_path;

    /**
     * Prepend the mount path to the filename.
     *
     * @param filepath
     * @return
     */
    const std::string WrapPath(const std::string &filepath) {
        return this->mount_path + filepath;
    }


public:

    DFSServiceImpl(const std::string &mount_path): mount_path(mount_path) {
    }

    ~DFSServiceImpl() {}

    //
    // STUDENT INSTRUCTION:
    //
    // Add your additional code here, including
    // implementations of your protocol service methods
    //
    Status FetchFile(ServerContext *context, const File *request, ServerWriter<FileData> *writer) override {
        //get path from file name
        const string &path = WrapPath(request->name());
        //check status of the path
        struct stat file_stat;
        if (stat(path.c_str(), &file_stat) != 0) {
            const string &err = "Fetch path check fail. Path: ";
            dfs_log(LL_ERROR) << err << path;
            return Status(StatusCode::NOT_FOUND, "File not found");
        }
        int file_size = file_stat.st_size;

        ifstream infile(path);
        FileData data;
        int bytes_sent = 0;
        char buffer[DFS_BUFFERSIZE];
        int bytes_to_send = 0;
        while(bytes_sent < file_size) {
            //check if client times out
            if (context->IsCancelled()) {
                const string &err = "Client timeout";
                dfs_log(LL_ERROR) << err;
                return Status(StatusCode::DEADLINE_EXCEEDED, err);
            }
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
        return Status::OK;
    }


    Status StoreFile(ServerContext *context, ServerReader<FileData> *reader, FileRet *response) override {
        //first get metadata and file name
        const multimap<grpc::string_ref, grpc::string_ref>& client_metadata = context->client_metadata();
        auto file_nameCM = client_metadata.find("file_name");

        //if file name does not exist, log error and return status
        if (file_nameCM == client_metadata.end()) {
            const string &err = "file name missing";
            dfs_log(LL_ERROR) << err;
            return Status(StatusCode::CANCELLED, err);
        }

        //file name to string
        auto file_name = string(file_nameCM->second.begin(), file_nameCM->second.end());
        //generate path using WrapPath
        const string &path = WrapPath(file_name);

        ofstream ofs;
        ofs.open(path);

        if (!ofs.is_open()) {
            const string &err = "Error opening file with ofstream";
            dfs_log(LL_ERROR) << err;
            return Status(StatusCode::INTERNAL, err);
        }
        

        //store the file from stream in FileData object. 
        //Track bytes read to ensure whole expected file is stored
        FileData data;
        while (reader->Read(&data)){

            //client timeout
            if (context->IsCancelled()) {
                const string &err = "Client timeout";
                dfs_log(LL_ERROR) << err;
                return Status(StatusCode::DEADLINE_EXCEEDED, err);
            }
            else {
                //contents to string
                const string &data_chunk = data.contents();
                ofs << data_chunk;
            }
        }
        ofs.close();

        //check status of the file
        struct stat file_stat;
        if (stat(path.c_str(), &file_stat) != 0) {
            const string &err = "Store file unsuccessful. Path: ";
            dfs_log(LL_ERROR) << err << path;
            return Status(StatusCode::NOT_FOUND, "File not found");
        }
        else {
            Timestamp* time_modified = new Timestamp(TimeUtil::TimeTToTimestamp(file_stat.st_mtime));
            response->set_allocated_time_modified(time_modified);
            response->set_file_name(path);
        }
        return Status::OK;
    }

    Status DeleteFile(ServerContext *context, const File *request, FileRet *response) override {
        //check status of the path
        struct stat file_stat;
        //Valid path check
        const string &path = WrapPath(request->name());
        
        if (stat(path.c_str(), &file_stat) != 0) {
            const string &err = "Delete path check fail. Path: ";
            dfs_log(LL_ERROR) << err << path;
            return Status(StatusCode::NOT_FOUND, "File not found");
        }

        // check for client cancel
        if (context->IsCancelled()) {
            const string &err = "Client timeout";
            dfs_log(LL_ERROR) << err;
            return Status(StatusCode::DEADLINE_EXCEEDED, err);
        }

        //delete
        remove(path.c_str());
        //modify file return values
        response->set_file_name(request->name());
        Timestamp *time_modified = new Timestamp(TimeUtil::TimeTToTimestamp(file_stat.st_mtime));
        response->set_allocated_time_modified(time_modified);
        return Status::OK;
    }

    Status ListFiles(ServerContext *context, const NoArg *request, FileList *response) override {
        DIR *directory = opendir(mount_path.c_str());
        //directory entry structure
        struct dirent *entry;
        
        //if fail to open, return UNAVAILABLE
        if (directory != NULL) {
            while ((entry = readdir(directory))) {
                // check for client cancel
                if (context->IsCancelled()) {
                    const string &err = "Client timeout";
                    dfs_log(LL_ERROR) << err;
                    return Status(StatusCode::DEADLINE_EXCEEDED, err);
                }

                //add file to the file list
                string name(entry->d_name);
                string path = WrapPath(name);

                // add return value
                FileRet *ret = response->add_ret();
                ret->set_file_name(name);

                //check status of the path
                struct stat file_stat;
                if (stat(path.c_str(), &file_stat) != 0) {
                    const string &err = "Delete path check fail. Path: ";
                    dfs_log(LL_ERROR) << err << path;
                    return Status(StatusCode::NOT_FOUND, "File not found");
                }

                ret->set_file_name(name);
                Timestamp* time_modified = new Timestamp(TimeUtil::TimeTToTimestamp(file_stat.st_mtime));
                ret->set_allocated_time_modified(time_modified);
            }
            closedir(directory);
            return Status::OK;
        }
        else {
            const string &err = "Open Directoryfail";
            dfs_log(LL_ERROR) << err;
            return Status(StatusCode::CANCELLED, err);
        }
    }

    Status GetStatus(
        ServerContext *context,
        const File *request,
        FileStatus *response
    ) override {
        // check for client cancel
        if (context->IsCancelled()) {
            const string &err = "Client timeout";
            dfs_log(LL_ERROR) << err;
            return Status(StatusCode::DEADLINE_EXCEEDED, err);
        }

        //get path
        string path = WrapPath(request->name());

        //check status of the path
        struct stat file_stat;
        if (stat(path.c_str(), &file_stat) != 0) {
            const string &err = "Delete path check fail. Path: ";
            dfs_log(LL_ERROR) << err << path;
            return Status(StatusCode::NOT_FOUND, "File not found");
        }

        //generate needed timestamps
        Timestamp* time_modified = new Timestamp(TimeUtil::TimeTToTimestamp(file_stat.st_mtime));
        Timestamp* time_created = new Timestamp(TimeUtil::TimeTToTimestamp(file_stat.st_ctime));

        response->set_name(path);
        response->set_size(file_stat.st_size);
        response->set_allocated_time_modified(time_created);
        response->set_allocated_time_created(time_modified);
        return Status::OK;
    }
};

//
// STUDENT INSTRUCTION:
//
// The following three methods are part of the basic DFSServerNode
// structure. You may add additional methods or change these slightly,
// but be aware that the testing environment is expecting these three
// methods as-is.
//
/**
 * The main server node constructor
 *
 * @param server_address
 * @param mount_path
 */
DFSServerNode::DFSServerNode(const std::string &server_address,
        const std::string &mount_path,
        std::function<void()> callback) :
    server_address(server_address), mount_path(mount_path), grader_callback(callback) {}

/**
 * Server shutdown
 */
DFSServerNode::~DFSServerNode() noexcept {
    dfs_log(LL_SYSINFO) << "DFSServerNode shutting down";
    this->server->Shutdown();
}

/** Server start **/
void DFSServerNode::Start() {
    DFSServiceImpl service(this->mount_path);
    ServerBuilder builder;
    builder.AddListeningPort(this->server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    this->server = builder.BuildAndStart();
    dfs_log(LL_SYSINFO) << "DFSServerNode server listening on " << this->server_address;
    this->server->Wait();
}

//
// STUDENT INSTRUCTION:
//
// Add your additional DFSServerNode definitions here
//

