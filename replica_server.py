# replica_server.py
# Written by Matthew Breach and Lily Hymes

""" 
Stores replicated file information

Command line:

python3 replica_server.py <ip> <port> <storage_dir> [-d]
"""

# Imports
import sys
import glob
import queue
import os
import random
import shutil
import threading
import argparse

# Thrift setup 
sys.path.append('gen-py')
sys.path.insert(0, glob.glob('../thrift/thrift-0.19.0/lib/py/build/lib*')[0])

from PA3 import replicaServer
from PA3.ttypes import FileInfo, ContactInfo, Request, Response, CompleteInfo
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

# Debug printing
DEBUG = 0
def dprint(msg: str):
    if DEBUG:
        print(msg, flush=True)

# Constants
MAX_CHUNK = 2048

# Replica Server Class
class ReplicaServerHandler():

    # ██╗███╗   ██╗██╗████████╗
    # ██║████╗  ██║██║╚══██╔══╝
    # ██║██╔██╗ ██║██║   ██║   
    # ██║██║╚██╗██║██║   ██║   
    # ██║██║ ╚████║██║   ██║   
    # ╚═╝╚═╝  ╚═══╝╚═╝   ╚═╝   

    def __init__(self, node_ip, node_port, storage_path):
        self.info = ContactInfo(node_ip, node_port)
        self.storage_path = storage_path

        # Ensure storage_path exists
        os.makedirs(self.storage_path, exist_ok=True)

        self.contained_files = []
        self.server_list = []
        self.NR = 0
        self.NW = 0
        self.coordinatorContact = None 
        self.role = None

        # Setup lock for coordinator to ensure sequential consistency
        self._coord_lock = threading.Lock()
        self._queue_lock = threading.Lock()

        # Import compute nodes from compute_nodes.txt
        self.import_compute_nodes()

        # Set up coordinator
        if (self.role == 1):
            self.setup_coordinator()

    def import_compute_nodes(self):
        """Reads compute_nodes.txt to determine list of nodes to connect to"""

        with open("compute_nodes.txt") as fp:
            self.NR, self.NW = map(int, fp.readline().strip().split(","))
            for row in fp:
                ip, port, role = row.strip().split(",")
                info = ContactInfo(ip, int(port))
                self.server_list.append(info)
                if role == '1':
                    self.coordinatorContact = info

        # This is the coordinator
        if self.info == self.coordinatorContact:
            self.role = 1

        # quorum validation --> Raise exception if invalid
        N = len(self.server_list)
        if not (self.NR + self.NW > N and self.NW > N / 2):
            raise ValueError(f"Invalid quorum sizes: NR={self.NR}, NW={self.NW}, N={N}")

    def setup_coordinator(self):
        dprint(f"Initializing Server as Coordinator")
        self.jobQueue = queue.Queue()
        self.chosenServers = None
        self.currentTask = None
        self.taskNumberAssigned = 0
        self.taskNumberProcessing = 0

    # ██╗  ██╗███████╗██╗     ██████╗ ███████╗██████╗ 
    # ██║  ██║██╔════╝██║     ██╔══██╗██╔════╝██╔══██╗
    # ███████║█████╗  ██║     ██████╔╝█████╗  ██████╔╝
    # ██╔══██║██╔══╝  ██║     ██╔═══╝ ██╔══╝  ██╔══██╗
    # ██║  ██║███████╗███████╗██║     ███████╗██║  ██║
    # ╚═╝  ╚═╝╚══════╝╚══════╝╚═╝     ╚══════╝╚═╝  ╚═╝

    # ███████╗██╗   ██╗███╗   ██╗ ██████╗████████╗██╗ ██████╗ ███╗   ██╗███████╗
    # ██╔════╝██║   ██║████╗  ██║██╔════╝╚══██╔══╝██║██╔═══██╗████╗  ██║██╔════╝
    # █████╗  ██║   ██║██╔██╗ ██║██║        ██║   ██║██║   ██║██╔██╗ ██║███████╗
    # ██╔══╝  ██║   ██║██║╚██╗██║██║        ██║   ██║██║   ██║██║╚██╗██║╚════██║
    # ██║     ╚██████╔╝██║ ╚████║╚██████╗   ██║   ██║╚██████╔╝██║ ╚████║███████║
    # ╚═╝      ╚═════╝ ╚═╝  ╚═══╝ ╚═════╝   ╚═╝   ╚═╝ ╚═════╝ ╚═╝  ╚═══╝╚══════╝

    def open_client(self, ip, port):
        """ Create thrift client by calling self.open_client to simplify thrift interaction """
        dprint(f"Opening connection to {ip}:{port}")
        sock = TSocket.TSocket(ip, port)
        sock.setTimeout(2000) # 2s timeout
        transport = TTransport.TBufferedTransport(sock)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = replicaServer.Client(protocol)
        transport.open()
        return client, transport
    
    def update_file_metadata(self, name, version):
        """ Update version number of a file """
        dprint(f"Updating file {name} to version {version}")
        for f in self.contained_files:
            if f.name == name:
                f.version = version
                return
            
        # Add file if not currently in contained files
        dprint(f"File {name} not currently in files")
        self.contained_files.append(FileInfo(name, version))

    def get_all_files(self):
        """Called by coordinator onto node to get all files"""
        return self.contained_files

    def get_version(self, filename):
        """Externally called from coordinator, returns info about a file"""
        for file in self.contained_files:
            if file.name == filename:
                return file.version
        return 0            
    
    def get_file_size(self, filename):
        """Externally Called From Another Server (not nessecarily coordinator)"""
        return os.path.getsize(f'{self.storage_path}/{filename}')
    
    def request_data(self, filename, offset, size):
        """Externally Called from Another Server (not nessecarily coordinator)"""
        with open(f'{self.storage_path}/{filename}', 'rb') as fp:
            fp.seek(offset)
            return fp.read(size)
        
    def copy_file(self, version, filename, ip, port):
        """Copies a given file from a another given node"""
        #Note: Used chatgpt to figure out how the thrift binary and binary read/write works (there is no thrift binary documentation)
        client, trans = self.open_client(ip, port)
        try:
            size = client.get_file_size(filename)
            with open(f'{self.storage_path}/{filename}', 'wb') as fout:
                for offset in range(0, size, MAX_CHUNK):
                    fout.write(client.request_data(filename, offset, MAX_CHUNK))
        finally:
            trans.close()
        self.update_file_metadata(filename, version)
    
    #  ██████╗ ██████╗  ██████╗ ██████╗ ██████╗ ██╗███╗   ██╗ █████╗ ████████╗ ██████╗ ██████╗ 
    # ██╔════╝██╔═══██╗██╔═══██╗██╔══██╗██╔══██╗██║████╗  ██║██╔══██╗╚══██╔══╝██╔═══██╗██╔══██╗
    # ██║     ██║   ██║██║   ██║██████╔╝██║  ██║██║██╔██╗ ██║███████║   ██║   ██║   ██║██████╔╝
    # ██║     ██║   ██║██║   ██║██╔══██╗██║  ██║██║██║╚██╗██║██╔══██║   ██║   ██║   ██║██╔══██╗
    # ╚██████╗╚██████╔╝╚██████╔╝██║  ██║██████╔╝██║██║ ╚████║██║  ██║   ██║   ╚██████╔╝██║  ██║
    #  ╚═════╝ ╚═════╝  ╚═════╝ ╚═╝  ╚═╝╚═════╝ ╚═╝╚═╝  ╚═══╝╚═╝  ╚═╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝
                                                                                            
    # ███████╗██╗   ██╗███╗   ██╗ ██████╗████████╗██╗ ██████╗ ███╗   ██╗███████╗               
    # ██╔════╝██║   ██║████╗  ██║██╔════╝╚══██╔══╝██║██╔═══██╗████╗  ██║██╔════╝               
    # █████╗  ██║   ██║██╔██╗ ██║██║        ██║   ██║██║   ██║██╔██╗ ██║███████╗               
    # ██╔══╝  ██║   ██║██║╚██╗██║██║        ██║   ██║██║   ██║██║╚██╗██║╚════██║               
    # ██║     ╚██████╔╝██║ ╚████║╚██████╗   ██║   ██║╚██████╔╝██║ ╚████║███████║               
    # ╚═╝      ╚═════╝ ╚═╝  ╚═══╝ ╚═════╝   ╚═╝   ╚═╝ ╚═════╝ ╚═╝  ╚═══╝╚══════╝               
                                                                                         
    def cord_list_files(self):
        """Called onto Coordinator"""
        fileList = self.contained_files[:]
        dprint(self.contained_files)

        allFiles = []
        allFiles.append(CompleteInfo(self.info, fileList))
        
        
        for server in self.server_list:
            if server == self.info:
                continue
            client, transport = self.open_client(server.ip, server.port)
            try:
                returnedFileList = client.get_all_files()
            finally:
                transport.close()
            allFiles.append(CompleteInfo(server, returnedFileList))
        return allFiles
    
    def cord_read_file(self, filename):
        """Internal Coordinator Function"""
        self.chosenServers = random.sample(self.server_list, self.NR)
        newest = Response(0, None)

        for server in self.chosenServers:
            client, transport = self.open_client(server.ip, server.port)
            try:
                version = client.get_version(filename)
            finally:
                transport.close()
            if version > newest.version:
                newest.version = version
                newest.contact = server
        return newest

    def cord_write_file(self, filename):
        """Internal Coordinator Function"""
        self.chosenServers = random.sample(self.server_list, self.NW)
        newest = Response(0, None)

        for server in self.chosenServers:
            client, transport = self.open_client(server.ip, server.port)
            try:
                version = client.get_version(filename)
            finally:
                transport.close()
            if version > newest.version:
                newest.version = version
                newest.contact = server
        return newest
    
    def insert_job(self, request):
        """Called from server, inserts a job"""

        # Assign each request its own task number incrementally
        self._queue_lock.acquire()

        taskNumber = self.taskNumberAssigned
        self.taskNumberAssigned += 1

        self._queue_lock.release()


        while(True):
            self._coord_lock.acquire()

            # Verifies sequential ordering 
            if (taskNumber == self.taskNumberProcessing):
                None
            else:
                self._coord_lock.release()
                continue

            dprint(f"Coordinator: processing {request.type} {request.filename}")
            if request.type == "read":
                return self.cord_read_file(request.filename)
            return self.cord_write_file(request.filename)

        # with self._coord_lock:
        #     dprint(f"Coordinator: processing {request.type} {request.filename}")
        #     if request.type == "read":
        #         return self.cord_read_file(request.filename)
        #     return self.cord_write_file(request.filename)

    # ██████╗ ███████╗██████╗ ██╗     ██╗ ██████╗ █████╗                        
    # ██╔══██╗██╔════╝██╔══██╗██║     ██║██╔════╝██╔══██╗                       
    # ██████╔╝█████╗  ██████╔╝██║     ██║██║     ███████║                       
    # ██╔══██╗██╔══╝  ██╔═══╝ ██║     ██║██║     ██╔══██║                       
    # ██║  ██║███████╗██║     ███████╗██║╚██████╗██║  ██║                       
    # ╚═╝  ╚═╝╚══════╝╚═╝     ╚══════╝╚═╝ ╚═════╝╚═╝  ╚═╝                       
                                                                            
    # ███████╗██╗   ██╗███╗   ██╗ ██████╗████████╗██╗ ██████╗ ███╗   ██╗███████╗
    # ██╔════╝██║   ██║████╗  ██║██╔════╝╚══██╔══╝██║██╔═══██╗████╗  ██║██╔════╝
    # █████╗  ██║   ██║██╔██╗ ██║██║        ██║   ██║██║   ██║██╔██╗ ██║███████╗
    # ██╔══╝  ██║   ██║██║╚██╗██║██║        ██║   ██║██║   ██║██║╚██╗██║╚════██║
    # ██║     ╚██████╔╝██║ ╚████║╚██████╗   ██║   ██║╚██████╔╝██║ ╚████║███████║
    # ╚═╝      ╚═════╝ ╚═╝  ╚═══╝ ╚═════╝   ╚═╝   ╚═╝ ╚═════╝ ╚═╝  ╚═══╝╚══════╝

    def list_files(self):
        """Externally Called From Client"""
        # Delegate to coordinator if necessary
        if self.info != self.coordinatorContact:
            client, transport = self.open_client(self.coordinatorContact.ip, self.coordinatorContact.port)
            try:
                response = client.cord_list_files()
            finally:
                transport.close()
            return response
        
        # Otherwise, we are coordinator, fall to cord_list_files
        return self.cord_list_files()

    def read_file(self, filename):
        """Externally Called From Client"""
        request = Request("read", filename)

        dprint("Read Operation: inserting job to coordinator")
        client, transport = self.open_client(self.coordinatorContact.ip, self.coordinatorContact.port)
        try:
            response = client.insert_job(request)
        finally:
            transport.close()
        
        # Ensure local copy is up‑to‑date
        local_version = self.get_version(filename)
        if local_version < response.version: # Update file if needed
            dprint(f"Read Operation: Copying File")
            self.copy_file(response.version, filename, response.contact.ip, response.contact.port)
        else:
            dprint(f"Local Copy Already Most Recent Version")
        
        # ACK
        client, transport = self.open_client(self.coordinatorContact.ip, self.coordinatorContact.port)
        try:
            client.finish_read()
        finally:
            transport.close()

        return f"{self.storage_path}/{filename}"

    def write_file(self, filename, filepath):
        """Externally Called from Client"""
        request = Request("write", filename)

        dprint("Write Operation: inserting job to coordinator")
        client, transport = self.open_client(self.coordinatorContact.ip, self.coordinatorContact.port)
        try:
            response = client.insert_job(request)
        finally:
            transport.close()

        # Update file version
        new_version = response.version + 1
        shutil.copy(filepath, f"{self.storage_path}/")
        self.update_file_metadata(filename, new_version)

        # Inform coordinator
        client, transport = self.open_client(self.coordinatorContact.ip, self.coordinatorContact.port)
        try:
            client.finish_write(new_version, filename, self.info.ip, self.info.port, self.info.ip, self.info.port)
        finally:
            transport.close()

    #  ██████╗ █████╗ ██╗     ██╗             
    # ██╔════╝██╔══██╗██║     ██║             
    # ██║     ███████║██║     ██║             
    # ██║     ██╔══██║██║     ██║             
    # ╚██████╗██║  ██║███████╗███████╗        
    #  ╚═════╝╚═╝  ╚═╝╚══════╝╚══════╝        
                                            
    # ██████╗  █████╗  ██████╗██╗  ██╗███████╗
    # ██╔══██╗██╔══██╗██╔════╝██║ ██╔╝██╔════╝
    # ██████╔╝███████║██║     █████╔╝ ███████╗
    # ██╔══██╗██╔══██║██║     ██╔═██╗ ╚════██║
    # ██████╔╝██║  ██║╚██████╗██║  ██╗███████║
    # ╚═════╝ ╚═╝  ╚═╝ ╚═════╝╚═╝  ╚═╝╚══════╝
                                        
    def finish_write(self, version, filename, ip, port, source_ip, source_port):
        if self.chosenServers:
            for server in self.chosenServers:
                if (source_ip, source_port) == (server.ip, server.port):
                    continue
                client, transport = self.open_client(server.ip, server.port)
                try:
                    client.copy_file(version, filename, ip, port)
                finally:
                    transport.close()
            self.chosenServers = None
        self.taskNumberProcessing += 1
        self._coord_lock.release()
        

    def finish_read(self):
        self.chosenServers = None
        self.taskNumberProcessing += 1
        self._coord_lock.release()

def run_replica_server(node_ip, node_port, storage_path):
    handler = ReplicaServerHandler(node_ip, node_port, storage_path)
    processor = replicaServer.Processor(handler)
    transport = TSocket.TServerSocket(host='0.0.0.0', port=node_port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadPoolServer(processor, transport, tfactory, pfactory)

    print(f'Replica Server running @ {node_port} (coord={handler.role == 1})')
    server.serve()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Replica Server in DFS Network")
    parser.add_argument("node_ip", type=str, help="ip for replica server")
    parser.add_argument("node_port", type=int, help="Port number for replica server")
    parser.add_argument("storage_path", type=str, help="path to store data in")
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debug output")

    args = parser.parse_args()
    if args.debug:
        DEBUG = 1
    
    run_replica_server(args.node_ip, args.node_port, args.storage_path)
