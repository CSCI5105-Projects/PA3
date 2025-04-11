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
from PA3.ttypes import FileInfo, ContactInfo, DataChunk, Request

from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

# Our Imported Libraries
import queue

DEBUG = 1

class ReplicaServerHandler():
    def __init__(self, node_ip, node_port):
        self.info = ContactInfo(node_ip, node_port)
        self.contained_files = []
        self.server_list = []
        self.NR = 0
        self.NW = 0
        self.coordinatorContact = None 
        self.role = None

        self.import_compute_nodes()

        if (self.role == 1):
            self.setup_coordinator()


    def import_compute_nodes(self):
        """Reads compute_nodes.txt to determine list of nodes to connect to"""

        with open("compute_nodes.txt") as file:
            self.NR, self.NW = file.readline().strip().split(",")

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

    def setup_coordinator(self):
        print(f"Initializing Server as Coordinator")
        self.jobQueue = queue.Queue()

    def get_all_files(self):
        """Called by coordinator onto node to get all files"""
        return self.contained_files

    def cord_list_files(self):
        """Called onto Coordinator"""
        fileList = self.contained_files
        
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

                for returnFile in returnedFileList:
                    
                    for file in fileList:
                        if returnFile.name == file.name:
                            if returnFile.version > file.version:
                                file.version = returnFile.version

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

    def get_version(self, filename):
        """Externally called from coordinator, returns info about a file"""
        for file in self.contained_files:
            if file.name == filename:
                return file

    def read_file(self, filename):
        """Externally Called From Client"""
        None

    def write_file(self, filename, filepath):
        """Externally Called from Client"""
        if self.role == 1:
            request = Request("write", filename, filepath)
            self.jobQueue.put(request)

        

        else:
            transport = TSocket.TSocket(self.coordinatorContact.ip, self.coordinatorContact.port)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = replicaServer.Client(protocol) 

            transport.open()

            client.write_file(filename, filepath)

            transport.close

        None

    def confirm_operation(self):
        """Externally Called From Client"""
        None


def run_replica_server(node_ip, node_port):
    handler = ReplicaServerHandler(node_ip, node_port)
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
    run_replica_server(args.node_ip, args.node_port)        