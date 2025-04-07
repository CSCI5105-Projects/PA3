// compile with thrift -r --gen py PA3.thrift
// /home/breac001/source/repos/thrift/thrift-0.19.0/compiler/cpp/thrift -r --gen py PA3.thrift

struct FileInfo {
    1: string name
    2: i32 version
}

struct ContactInfo {
    1: string ip
    2: i32 port
}

service replicaServer {

    # For client to call
    list<FileInfo> list_files()
    string read_file(1:string filename)
    #TODO void write_file(1: string filename)
    void confirm_operation() 

    # For coordinator to call
    FileInfo get_version(1: string filename) # Called by coordinator to get version from a server
    list<FileInfo> get_all_files() # Called by a coordinator on a node to get a list of files on it

    # Called on Coordinator
    list<FileInfo> cord_list_files()


    # For Sending Data Around
    i32 get_file_size(1: string filename)
    # TODO: actual data moving

}