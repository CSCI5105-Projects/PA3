# replica_server.py
# Written by Matthew Breach and Lily Hymes

""" 
Stores replicated file information

Command line:

python3 replica_server.py <ip> <port>
"""

import sys
import glob

sys.path.append('gen-py')
sys.path.insert(0, glob.glob('../thrift/thrift-0.19.0/lib/py/build/lib*')[0])

# Thrift Libraries
from PA3 import replicaServer
from PA3.ttypes import FileInfo, ContactInfo, Request, Response

from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

# Our Imported Libraries
import queue
import os
import random
import shutil

DEBUG = 1

class ReplicaServerHandler():
    def __init__(self, node_ip, node_port, storage_path):
        self.info = ContactInfo(node_ip, node_port)
        self.storage_path = storage_path
        self.contained_files = []
        self.server_list = []
        self.NR = 0
        self.NW = 0
        self.coordinatorContact = None 
        self.role = None
        

        self.import_compute_nodes()

        #print(self.get_file_size("beemoviescript.txt"))

        if (self.role == 1):
            self.setup_coordinator()

        # else:
        #     self.copy_file("beemoviescript.txt", "127.0.0.1", 9090)
        #     self.copy_file("chicken_jockey.jpg", "127.0.0.1", 9090)


    def import_compute_nodes(self):
        """Reads compute_nodes.txt to determine list of nodes to connect to"""

        with open("compute_nodes.txt") as file:
            self.NR, self.NW = file.readline().strip().split(",")
            self.NR = int(self.NR)
            self.NW = int(self.NW)

            for row in file:
                ip, port, role = row.strip().split(",")
                port = int(port)
                role = int(role)
                info = ContactInfo(ip, port)
                self.server_list.append(info)
                if role == 1:
                    self.coordinatorContact = info

        if self.info == self.coordinatorContact:
            self.role = 1

        file.close()

    def setup_coordinator(self):
        print(f"Initializing Server as Coordinator")
        self.jobQueue = queue.Queue()
        self.chosenServers = None
        self.currentTask = None
        self.writePending = 0

    def get_all_files(self):
        """Called by coordinator onto node to get all files"""
        return self.contained_files

    def cord_list_files(self):
        """Called onto Coordinator"""
        fileList = self.contained_files[:]
        print(self.contained_files)
        
        for server in self.server_list:
            if server == self.info:
                None
            else:
                transport = TSocket.TSocket(server.ip, server.port)
                transport = TTransport.TBufferedTransport(transport)
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                client = replicaServer.Client(protocol)

                transport.open()

                returnedFileList = client.get_all_files()

                transport.close()

                #TODO: This way sucks, but does work, maybe find another way

                for returnFile in returnedFileList:
                    
                    for file in fileList:
                        if returnFile.name == file.name:
                            if returnFile.version > file.version:
                                file.version = returnFile.version
                            break
                    else:
                        fileList.append(returnFile)

        return fileList

    def list_files(self):
        """Externally Called From Client"""

        # Contact Coordinator, asking for list of files
        transport = TSocket.TSocket(self.coordinatorContact.ip, self.coordinatorContact.port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = replicaServer.Client(protocol)

        transport.open()

        returnVal = client.cord_list_files()

        transport.close

        # Return List of Files
        return returnVal

    def cord_read_file(self, filename):
        """Internal Coordinator Function"""
        self.chosenServers = random.sample(self.server_list, self.NR)

        returnVal = Response(0, None)

        for server in self.chosenServers:
            transport = TSocket.TSocket(server.ip, server.port)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = replicaServer.Client(protocol) 

            transport.open()

            version = client.get_version(filename)
            if version > returnVal.version:
                returnVal.version = version
                returnVal.contact = server

            transport.close()

        self.chosenServers = None

        return returnVal

    def cord_write_file(self, filename):
        """Internal Coordinator Function"""
        self.chosenServers = random.sample(self.server_list, self.NW)

        returnVal = Response(0, None)

        for server in self.chosenServers:
            transport = TSocket.TSocket(server.ip, server.port)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = replicaServer.Client(protocol) 

            transport.open()

            version = client.get_version(filename)
            if version > returnVal.version:
                returnVal.version = version
                returnVal.contact = server

            transport.close()



        return returnVal

    def insert_job(self, request):
        """Called from server, inserts a job"""
        #TODO: Finish this. Should be all the Mutex and stuff in here
        if DEBUG > 0:
            print(f"Coordinator: Receiving Job of Type: {request.type}, adding to queue")
        self.jobQueue.put(request)

        if DEBUG > 0:
            print(f"Coordinator: Ready to Run Job")
        self.currentTask = self.jobQueue.get()

        if (self.currentTask.type == "read"):
            response = self.cord_read_file(self.currentTask.filename)
        elif (self.currentTask.type == "write"):
            response = self.cord_write_file(self.currentTask.filename)
            self.writePending = 1

        return response

    def get_version(self, filename):
        """Externally called from coordinator, returns info about a file"""
        for file in self.contained_files:
            if file.name == filename:
                returnVal = file.version
                break
        else:
            returnVal = 0
        
        return returnVal            

    def read_file(self, filename):
        """Externally Called From Client"""
        request = Request("read", filename)

        transport = TSocket.TSocket(self.coordinatorContact.ip, self.coordinatorContact.port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = replicaServer.Client(protocol) 

        transport.open()
        if DEBUG > 0:
            print(f"Read Operation: Inserting Job")
        response = client.insert_job(request)

        transport.close()
        if DEBUG > 0:
            print(f"Read Operation: Copying File")
        for file in self.contained_files:
            if file.name == filename:
                print(f"file.version: {file.version}")
                print(f"repsonse.version: {response.version}")

                if file.version < response.version:
                    # has the file, but not most recent version
                    self.copy_file(response.version, filename, response.contact.ip, response.contact.port)
                else:
                    # Has Most Recent Version Already
                    break
        else:
            # Does not have any version of the file
            self.copy_file(response.version, filename, response.contact.ip, response.contact.port)
        
        path = self.storage_path + "/" + filename
        return path

    def write_file(self, filename, filepath):
        """Externally Called from Client"""
        request = Request("write", filename)

        transport = TSocket.TSocket(self.coordinatorContact.ip, self.coordinatorContact.port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = replicaServer.Client(protocol) 

        transport.open()

        if DEBUG > 0:
            print(f"Write Operation: Inserting Job")
        response = client.insert_job(request)

        # Increment stored version by 1
        for file in self.contained_files:
            if file.name == filename:
                file.version = response.version + 1
                break
        else:
            self.contained_files.append(FileInfo(filename, response.version+1))

        if DEBUG > 0:
            print(f"Write Operation: Copying Supplied File {filename} to internal storage")
        destination = self.storage_path + "/"
        shutil.copy(filepath, destination)

        if DEBUG > 0:
            print(f"Write Operation: Informing Coordinator Write has finished")
        client.finish_write(response.version+1, filename, self.info.ip, self.info.port, self.info.ip, self.info.port)

        transport.close()

        return

    def finish_write(self, version, filename, ip, port, source_ip, source_port):

        for server in self.chosenServers:
            if source_ip == server.ip and source_port == server.port:
                continue

            transport = TSocket.TSocket(server.ip, server.port)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = replicaServer.Client(protocol)

            transport.open()

            client.copy_file(version, filename, ip, port)

            transport.close()

        self.chosenServers = None
        self.writePending = 0

    def confirm_operation(self):
        """Externally Called From Client"""
        None

    def get_file_size(self, filename):
        """Externally Called From Another Server (not nessecarily coordinator)"""
        path = self.storage_path + "/" + filename
        return os.path.getsize(path)
    
    def request_data(self, filename, offset, size):
        """Externally Called from Another Server (not nessecarily coordinator)"""
        path = self.storage_path + "/" + filename
        with open(path, 'rb') as file:
            file.seek(offset)
            return file.read(size)
        
    def copy_file(self, version, filename, ip, port):
        """Copies a given file from a another given node"""
        #Note: Used chatgpt to figure out how the thrift binary and binary read/write works (there is no thrift binary documentation)
        transport = TSocket.TSocket(ip, port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = replicaServer.Client(protocol)

        transport.open()

        fileSize = client.get_file_size(filename)
        chunkSize = 2048
        localPath = self.storage_path + "/" + filename
        with open(localPath, "wb") as file:
            for offset in range (0, fileSize, chunkSize):
                chunk = client.request_data(filename, offset, chunkSize)
                file.write(chunk)

        transport.close()

        # Updates Stored File Information
        for file in self.contained_files:
            if file.name == filename:
                file.version = version
                return
        
        self.contained_files.append(FileInfo(filename, version))
        return

def run_replica_server(node_ip, node_port, storage_path):
    handler = ReplicaServerHandler(node_ip, node_port, storage_path)
    processor = replicaServer.Processor(handler)
    transport = TSocket.TServerSocket(host='0.0.0.0', port=node_port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadPoolServer(processor, transport, tfactory, pfactory)

    print(f"Replica Server Running on Port: {node_port}")
    server.serve()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Replica Server in DFS Network")
    parser.add_argument("node_ip", type=str, help="ip for replica server")
    parser.add_argument("node_port", type=int, help="Port number for replica server")
    parser.add_argument("storage_path", type=str, help="path to store data in")
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debug output")


    args = parser.parse_args()
    if args.debug:
        DEBUG = 1
    run_replica_server(args.node_ip, args.node_port, args.storage_path)        