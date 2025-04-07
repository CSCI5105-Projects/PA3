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
from PA3.ttypes import FileInfo, ContactInfo


from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

DEBUG = 1

class ReplicaServerHandler():
    def __init__(self, node_ip, node_port):
        self.info = ContactInfo(node_ip, node_port)
        self.contained_files = []
        self.server_list = []
        self.NR = 0
        self.NW = 0

        self.import_compute_nodes()


    def import_compute_nodes(self):
        """Reads compute_nodes.txt to determine list of nodes to connect to"""
        None

    def list_files(self):
        """Externally Called From Client"""
        returnVal = []
        return returnVal

    def read_file(filename):
        """Externally Called From Client"""
        None

    def confirm_operation():
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
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debug output")


    args = parser.parse_args()
    if args.debug:
        DEBUG = 1
    run_replica_server(args.node_ip, args.node_port)        