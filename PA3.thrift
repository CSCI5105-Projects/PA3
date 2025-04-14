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



struct Request{
    1:string type
    2:string filename
}

struct Response {
    1:i32 version
    2:ContactInfo contact
}

service replicaServer {

    # For client to call
    list<FileInfo> list_files()
    string read_file(1:string filename)
    void write_file(1:string filename, 2:string filepath)
    void confirm_operation() 

    # For coordinator to call
    i32 get_version(1: string filename) # Called by coordinator to get version from a server
    list<FileInfo> get_all_files() # Called by a coordinator on a node to get a list of files on it
    void node_write_file(1: string filename, 2:string filepath, 3:i32 version)

    # Called on Coordinator
    Response insert_job(1: Request request)
    void finish_write(1:i32 version, 2:string filename, 3:string ip, 4:i32 port, 5:string source_ip, 6:i32 source_port)
    list<FileInfo> cord_list_files()


    # For Sending Data Around
    i64 get_file_size(1: string filename)
    binary request_data(1: string filename, 2:i32 offest, 3:i32 size)
    void copy_file(1:i32 version, 2:string filename, 3:string ip, 4:i32 port)

}