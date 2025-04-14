# client.py
# Written by Matthew Breach and Lily Hymes

"""
Client for reading and writing to replica server

Command line:

python3 client.py <server_ip> <server_port>
"""

import sys
import glob
sys.path.append('gen-py')
sys.path.insert(0, glob.glob('../thrift/thrift-0.19.0/lib/py/build/lib*')[0])

# Thrift libraries
from PA3 import replicaServer
from PA3.ttypes import FileInfo, ContactInfo

from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol

import argparse

def list_files(server_ip, server_port):
    transport = TSocket.TSocket(server_ip, server_port)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = replicaServer.Client(protocol)

    transport.open()

    fileList = client.list_files()

    print(f"Listing Files: ")

    for file in fileList:
        print(f"File: {file.name}, Version: {file.version}")

    transport.close()

def read_file(server_ip, server_port, filename):
    transport = TSocket.TSocket(server_ip, server_port)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = replicaServer.Client(protocol)

    transport.open()

    filepath = client.read_file(filename)

    print(f"Pretend you're reading a file: {filename} at {filepath}")

    print(f"Done reading file")

    #client.confirm_operation()

    transport.close()

def write_file(server_ip, server_port, filename, filepath):
    transport = TSocket.TSocket(server_ip, server_port)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = replicaServer.Client(protocol)

    transport.open()

    print(f"Writing File: {filename} at path: {filepath}")

    client.write_file(filename, filepath)

    #client.confirm_operation()

    transport.close()

def main():
    parser = argparse.ArgumentParser(description="Client for reading and writing")
    parser.add_argument("server_ip", type=str, help="Server IP address")
    parser.add_argument("server_port", type=int, help="Server port")
    parser.add_argument("-l", "--list", action="store_true", help = "List all files and versions")
    parser.add_argument("-r", "--read", help = "Read a file with given filename")
    parser.add_argument("-w", "--write" ,nargs=2, help="Write a file with given filename and filepath")
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debug output")

    args = parser.parse_args()
    if args.debug:
        DEBUG = 1

    if args.list:
        list_files(args.server_ip, args.server_port)

    elif args.read:
        read_file(args.server_ip, args.server_port, args.read)

    elif args.write:
        write_file(args.server_ip, args.server_port, args.write[0], args.write[1])

if __name__ == "__main__":
    main()