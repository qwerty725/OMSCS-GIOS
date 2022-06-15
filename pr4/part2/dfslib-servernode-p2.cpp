#include <map>
#include <mutex>
#include <shared_mutex>
#include <chrono>
#include <string>
#include <thread>
#include <errno.h>
#include <iostream>
#include <cstdio>
#include <fstream>
#include <getopt.h>
#include <sys/stat.h>
#include <grpcpp/grpcpp.h>
#include <dirent.h>

#include "src/dfslibx-service-runner.h"
#include "dfslib-shared-p2.h"
#include "proto-src/dfs-service.grpc.pb.h"
#include "src/dfslibx-call-data.h"
#include "dfslib-servernode-p2.h"
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
// Change these "using" aliases to the specific
// message types you are using in your `dfs-service.proto` file
// to indicate a file request and a listing of files from the server
//
using FileRequestType = File;
using FileListResponseType = FileList;

extern dfs_log_level_e DFS_LOG_LEVEL;

//
// STUDENT INSTRUCTION:
//
// As with Part 1, the DFSServiceImpl is the implementation service for the rpc methods
// and message types you defined in your `dfs-service.proto` file.
//
// You may start with your Part 1 implementations of each service method.
//
// Elements to consider for Part 2:
//
// - How will you implement the write lock at the server level?
// - How will you keep track of which client has a write lock for a file?
//      - Note that we've provided a preset client_id in DFSClientNode that generates
//        a client id for you. You can pass that to the server to identify the current client.
// - How will you release the write lock?
// - How will you handle a store request for a client that doesn't have a write lock?
// - When matching files to determine similarity, you should use the `file_checksum` method we've provided.
//      - Both the client and server have a pre-made `crc_table` variable to speed things up.
//      - Use the `file_checksum` method to compare two files, similar to the following:
//
//          std::uint32_t server_crc = dfs_file_checksum(filepath, &this->crc_table);
//
//      - Hint: as the crc checksum is a simple integer, you can pass it around inside your message types.
//
class DFSServiceImpl final :
    public DFSService::WithAsyncMethod_CallbackList<DFSService::Service>,
        public DFSCallDataManager<FileRequestType , FileListResponseType> {

private:

    /** The runner service used to start the service and manage asynchronicity **/
    DFSServiceRunner<FileRequestType, FileListResponseType> runner;

    /** Mutex for managing the queue requests **/
    std::mutex queue_mutex;

    /** The mount path for the server **/
    std::string mount_path;

    /** The vector of queued tags used to manage asynchronous requests **/
    std::vector<QueueRequest<FileRequestType, FileListResponseType>> queued_tags;

    
    /**
     * Prepend the mount path to the filename.
     *
     * @param filepath
     * @return
     */
    const std::string WrapPath(const std::string &filepath) {
        return this->mount_path + filepath;
    }

    //file to client id map and mutex
    map<string, string> file2ClientMap;
    mutex file2ClientMap_mutex;
    
    /** CRC Table kept in memory for faster calculations **/
    CRC::Table<std::uint32_t, 32> crc_table;

public:

    DFSServiceImpl(const std::string& mount_path, const std::string& server_address, int num_async_threads):
        mount_path(mount_path), crc_table(CRC::CRC_32()) {

        this->runner.SetService(this);
        this->runner.SetAddress(server_address);
        this->runner.SetNumThreads(num_async_threads);
        this->runner.SetQueuedRequestsCallback([&]{ this->ProcessQueuedRequests(); });

    }

    ~DFSServiceImpl() {
        this->runner.Shutdown();
    }

    void Run() {
        this->runner.Run();
    }

    /**
     * Request callback for asynchronous requests
     *
     * This method is called by the DFSCallData class during
     * an asynchronous request call from the client.
     *
     * Students should not need to adjust this.
     *
     * @param context
     * @param request
     * @param response
     * @param cq
     * @param tag
     */
    void RequestCallback(grpc::ServerContext* context,
                         FileRequestType* request,
                         grpc::ServerAsyncResponseWriter<FileListResponseType>* response,
                         grpc::ServerCompletionQueue* cq,
                         void* tag) {

        std::lock_guard<std::mutex> lock(queue_mutex);
        this->queued_tags.emplace_back(context, request, response, cq, tag);

    }

    /**
     * Process a callback request
     *
     * This method is called by the DFSCallData class when
     * a requested callback can be processed. You should use this method
     * to manage the CallbackList RPC call and respond as needed.
     *
     * See the STUDENT INSTRUCTION for more details.
     *
     * @param context
     * @param request
     * @param response
     */
    void ProcessCallback(ServerContext* context, FileRequestType* request, FileListResponseType* response) {

        //
        // STUDENT INSTRUCTION:
        //
        // You should add your code here to respond to any CallbackList requests from a client.
        // This function is called each time an asynchronous request is made from the client.
        //
        // The client should receive a list of files or modifications that represent the changes this service
        // is aware of. The client will then need to make the appropriate calls based on those changes.
        //
        Status callback_status = this->CallbackList(context, request, response);
        if(callback_status.ok()) {
            dfs_log(LL_SYSINFO) << "CallbackList OK";
        }
        else{
            dfs_log(LL_ERROR) << "CallbackList error" << callback_status.error_message();
        }
    }

    /**
     * Processes the queued requests in the queue thread
     */
    void ProcessQueuedRequests() {
        while(true) {

            //
            // STUDENT INSTRUCTION:
            //
            // You should add any synchronization mechanisms you may need here in
            // addition to the queue management. For example, modified files checks.
            //
            // Note: you will need to leave the basic queue structure as-is, but you
            // may add any additional code you feel is necessary.
            //


            // Guarded section for queue
            {
                dfs_log(LL_DEBUG2) << "Waiting for queue guard";
                std::lock_guard<std::mutex> lock(queue_mutex);


                for(QueueRequest<FileRequestType, FileListResponseType>& queue_request : this->queued_tags) {
                    this->RequestCallbackList(queue_request.context, queue_request.request,
                        queue_request.response, queue_request.cq, queue_request.cq, queue_request.tag);
                    queue_request.finished = true;
                }

                // any finished tags first
                this->queued_tags.erase(std::remove_if(
                    this->queued_tags.begin(),
                    this->queued_tags.end(),
                    [](QueueRequest<FileRequestType, FileListResponseType>& queue_request) { return queue_request.finished; }
                ), this->queued_tags.end());

            }
        }
    }

    //
    // STUDENT INSTRUCTION:
    //
    // Add your additional code here, including
    // the implementations of your rpc protocol methods.
    //
    Status FetchFile(ServerContext *context, const File *request, ServerWriter<FileData> *writer) override {
        const std::multimap<grpc::string_ref, grpc::string_ref> metadata = context->client_metadata();
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

        //check crc
        auto clientCRC_MD = metadata.find("crc");
        if (clientCRC_MD == metadata.end()) {
            const string &err = "Store file client crc missing";
            dfs_log(LL_ERROR) << err;
            return Status(StatusCode::CANCELLED, err);
        }
        //get client crc
        unsigned long client_CRC = stoul(string(clientCRC_MD->second.begin(), clientCRC_MD->second.end()));
        //get server CRC
        unsigned long server_CRC = dfs_file_checksum(path, &crc_table);
        //compare
        if (client_CRC == server_CRC) {
            //if they are the same, it means the file store attempt is the same as on server already
            const string &err = "Store file already exists";
            dfs_log(LL_SYSINFO) << err;
            return Status(StatusCode::ALREADY_EXISTS, err);
        }
        //ENDCRC CHECK

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
        auto clientIDCM = client_metadata.find("client_id");


        //if file name does not exist, log error and return status
        if (file_nameCM == client_metadata.end()) {
            const string &err = "file name missing";
            dfs_log(LL_ERROR) << err;
            return Status(StatusCode::CANCELLED, err);
        }

        if (clientIDCM == client_metadata.end()){
            const string &err = "Client id missing";
            dfs_log(LL_ERROR) << err;
            return Status(StatusCode::CANCELLED, err);
        }

        //file name to string
        auto file_name = string(file_nameCM->second.begin(), file_nameCM->second.end());
        //client id to string
        auto client_id = string(clientIDCM->second.begin(), clientIDCM->second.end());

        //generate path using WrapPath
        const string &path = WrapPath(file_name);


        //check crc
        auto clientCRC_MD = client_metadata.find("crc");
        if (clientCRC_MD == client_metadata.end()) {
            const string &err = "Store file client crc missing";
            dfs_log(LL_ERROR) << err;
            return Status(StatusCode::CANCELLED, err);
        }
        //get client crc
        unsigned long client_CRC = stoul(string(clientCRC_MD->second.begin(), clientCRC_MD->second.end()));
        //get server CRC
        unsigned long server_CRC = dfs_file_checksum(path, &crc_table);
        //compare
        if (client_CRC == server_CRC) {
            //if they are the same, it means the file store attempt is the same as on server already
            const string &err = "Store file already exists";
            dfs_log(LL_SYSINFO) << err;
            return Status(StatusCode::ALREADY_EXISTS, err);
        }
        //ENDCRC CHECK


        //check write lock
        Status file2ClientMap_status = file2ClientMapLockCheck(client_id, file_name);
        if (!file2ClientMap_status.ok()) {
            const string &err = "Write lock error";
            dfs_log(LL_SYSINFO) << err;
            return Status(StatusCode::INTERNAL, err);
        }


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
        //release lock
        file2ClientMapRelease(file_name);
        return Status::OK;
    }

    Status DeleteFile(ServerContext *context, const File *request, FileRet *response) override {
        //first get metadata and file name
        const multimap<grpc::string_ref, grpc::string_ref>& client_metadata = context->client_metadata();
        auto clientIDCM = client_metadata.find("client_id");


        //if client does not exist, log error and return status

        if (clientIDCM == client_metadata.end()){
            const string &err = "Client id missing";
            dfs_log(LL_ERROR) << err;
            return Status(StatusCode::CANCELLED, err);
        }

        //client id to string
        auto client_id = string(clientIDCM->second.begin(), clientIDCM->second.end());
        //check lock status for the file
        Status file2ClientMap_status = file2ClientMapLockCheck(client_id, request->name());
        if (!file2ClientMap_status.ok()) {
            const string &err = "Write lock error";
            dfs_log(LL_SYSINFO) << err;
            return Status(StatusCode::INTERNAL, err);
        }

        //check status of the path
        struct stat file_stat;
        //Valid path check
        const string &path = WrapPath(request->name());
        
        if (stat(path.c_str(), &file_stat) != 0) {
            file2ClientMapRelease(request->name());
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
        file2ClientMapRelease(request->name());
        return Status::OK;
    }

    Status ListFiles(ServerContext *context, const NoArg *request, FileList *response) override {
        DIR *directory = opendir(mount_path.c_str());
        //directory entry structure
        struct dirent *entry;
        
        //if fail to open, return UNAVAILABLE
        if (directory != NULL) {
            while ((entry = readdir(directory))) {

                //add file to the file list
                string name(entry->d_name);
                string path = WrapPath(name);

                // add return value
                FileStatus *ret = response->add_file_status();
                ret->set_name(name);

                //check status of the path
                struct stat file_stat;
                if (stat(path.c_str(), &file_stat) != 0) {
                    const string &err = "Delete path check fail. Path: ";
                    dfs_log(LL_ERROR) << err << path;
                    return Status(StatusCode::NOT_FOUND, "File not found");
                }

                ret->set_name(name);
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

    Status GetStatus(ServerContext *context,const File *request,FileStatus *response) override {
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

    Status CallbackList(ServerContext *context, const File *request, FileList *response) override {
        dfs_log(LL_SYSINFO) << "CallbackList ";
        NoArg *req;
        return this->ListFiles(context, req, response);
    }

    Status WriteLock(ServerContext *context, const File *request, FileRet *response) override {
        //Get client id
        const multimap<grpc::string_ref, grpc::string_ref>& client_metadata = context->client_metadata();
        auto clientIDCM = client_metadata.find("client_id");

        if (clientIDCM == client_metadata.end()){
            const string &err = "Client id missing";
            dfs_log(LL_ERROR) << err;
            return Status(StatusCode::CANCELLED, err);
        }

        //client id to string
        auto client_id = string(clientIDCM->second.begin(), clientIDCM->second.end());

        //perform same steps as lock check but insert if file doesn't exist
        lock_guard<mutex> lock(file2ClientMap_mutex);

        auto fileMapEntry = file2ClientMap.find(request->name());
        if(fileMapEntry == file2ClientMap.end()) {
            //no client tied to this file
            //add the mapping here to "lock" the file
            file2ClientMap.insert(pair<string, string>(request->name(), client_id));
            return Status::OK;
        }
        //if there is an existing map 
        string clientID = string(fileMapEntry->second.begin(), fileMapEntry->second.end());
        if (clientID == client_id) {
            //client is already tied to the file
            return Status::OK;
        }
        else{
            return Status::CANCELLED;
        }
    }
    Status file2ClientMapLockCheck(const string &client_id, const string &file_name) {
        //check map for any client_id to file_name pairing
        //each file name can only have one client_id so a map of name to ID works
        //check the map for the file name entry. if it doesnt' exist, add it and return OK status
        //if it does exist, it means the file is currently being accessed by a client
        lock_guard<mutex> lock(file2ClientMap_mutex);

        auto fileMapEntry = file2ClientMap.find(file_name);
        if(fileMapEntry == file2ClientMap.end()) {
            //no client tied to this file
            return Status::OK;
        }
        string clientID = string(fileMapEntry->second.begin(), fileMapEntry->second.end());
        if (clientID == client_id) {
            //client is already tied to the file
            return Status::OK;
        }
        else{
            return Status::CANCELLED;
        }
    }

    void file2ClientMapRelease(const string &file_name){
        //remove file from the map indicating there is no client currently tied to the file
        lock_guard<mutex> lock(file2ClientMap_mutex);
        file2ClientMap.erase(file_name);
    }
};

//
// STUDENT INSTRUCTION:
//
// The following three methods are part of the basic DFSServerNode
// structure. You may add additional methods or change these slightly
// to add additional startup/shutdown routines inside, but be aware that
// the basic structure should stay the same as the testing environment
// will be expected this structure.
//
/**
 * The main server node constructor
 *
 * @param mount_path
 */
DFSServerNode::DFSServerNode(const std::string &server_address,
        const std::string &mount_path,
        int num_async_threads,
        std::function<void()> callback) :
        server_address(server_address),
        mount_path(mount_path),
        num_async_threads(num_async_threads),
        grader_callback(callback) {}
/**
 * Server shutdown
 */
DFSServerNode::~DFSServerNode() noexcept {
    dfs_log(LL_SYSINFO) << "DFSServerNode shutting down";
}

/**
 * Start the DFSServerNode server
 */
void DFSServerNode::Start() {
    DFSServiceImpl service(this->mount_path, this->server_address, this->num_async_threads);


    dfs_log(LL_SYSINFO) << "DFSServerNode server listening on " << this->server_address;
    service.Run();
}

//
// STUDENT INSTRUCTION:
//
// Add your additional definitions here
//
