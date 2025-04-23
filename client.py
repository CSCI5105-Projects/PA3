# client.py
# Written by Matthew Breach and Lily Hymes

"""
Client for reading and writing to replica server

Usage: 
    python3 client.py [-h] [-l] [-r READ] [-w WRITE WRITE] [-d] server_ip server_port

    Client for reading and writing

    Positional arguments:
        server_ip             Server IP address
        server_port           Server port

    Options:
        -h, --help            show this help message and exit
        -l, --list            List all files and versions
        -r READ, --read READ  Read a file with given filename
        -w WRITE WRITE, --write WRITE WRITE
                                Write a file with given filename and filepath
        -d, --debug           Enable debug output
"""

#  ██████╗ ██████╗ ███╗   ██╗███████╗██╗ ██████╗ 
# ██╔════╝██╔═══██╗████╗  ██║██╔════╝██║██╔════╝ 
# ██║     ██║   ██║██╔██╗ ██║█████╗  ██║██║  ███╗
# ██║     ██║   ██║██║╚██╗██║██╔══╝  ██║██║   ██║
# ╚██████╗╚██████╔╝██║ ╚████║██║     ██║╚██████╔╝
#  ╚═════╝ ╚═════╝ ╚═╝  ╚═══╝╚═╝     ╚═╝ ╚═════╝                          

# Imports
import sys, glob, argparse

# Thrift setup 
sys.path.append('gen-py')
sys.path.insert(0, glob.glob('../thrift/thrift-0.19.0/lib/py/build/lib*')[0])

from PA3 import replicaServer
from PA3.ttypes import FileInfo, ContactInfo, CompleteInfo
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol

# Debug printing
DEBUG = 0
def dprint(msg: str):
    if DEBUG:
        print(msg, flush=True)

#  ██╗  ██╗███████╗██╗     ██████╗ ███████╗██████╗ ███████╗
#  ██║  ██║██╔════╝██║     ██╔══██╗██╔════╝██╔══██╗██╔════╝
#  ███████║█████╗  ██║     ██████╔╝█████╗  ██████╔╝███████╗
#  ██╔══██║██╔══╝  ██║     ██╔═══╝ ██╔══╝  ██╔══██╗╚════██║
#  ██║  ██║███████╗███████╗██║     ███████╗██║  ██║███████║
#  ╚═╝  ╚═╝╚══════╝╚══════╝╚═╝     ╚══════╝╚═╝  ╚═╝╚══════╝

def open_client(ip, port):
    """ Create thrift client by calling self.open_client to simplify thrift interaction """
    dprint(f"Opening connection to {ip}:{port}")
    sock = TSocket.TSocket(ip, port)
    # sock.setTimeout(2000) #2s timeout
    transport = TTransport.TBufferedTransport(sock)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = replicaServer.Client(protocol)
    transport.open()
    return client, transport

#  ███████╗███████╗██████╗ ██╗   ██╗███████╗██████╗ 
#  ██╔════╝██╔════╝██╔══██╗██║   ██║██╔════╝██╔══██╗
#  ███████╗█████╗  ██████╔╝██║   ██║█████╗  ██████╔╝
#  ╚════██║██╔══╝  ██╔══██╗╚██╗ ██╔╝██╔══╝  ██╔══██╗
#  ███████║███████╗██║  ██║ ╚████╔╝ ███████╗██║  ██║
#  ╚══════╝╚══════╝╚═╝  ╚═╝  ╚═══╝  ╚══════╝╚═╝  ╚═╝
#                                                   
#   ██████╗ █████╗ ██╗     ██╗     ███████╗         
#  ██╔════╝██╔══██╗██║     ██║     ██╔════╝         
#  ██║     ███████║██║     ██║     ███████╗         
#  ██║     ██╔══██║██║     ██║     ╚════██║         
#  ╚██████╗██║  ██║███████╗███████╗███████║         
#   ╚═════╝╚═╝  ╚═╝╚══════╝╚══════╝╚══════╝         

def list_files(ip, port):
    client, transport = open_client(ip, port)

    try:
        files = client.list_files()
    finally:
        transport.close()

    for server in files:
        print(f"Server: {server.contact.ip}, {server.contact.port}, Stored Files: ")
        for file in server.files:
            print(f"{file.name}  (v{file.version})")

def read_file(ip, port, filename):
    client, transport = open_client(ip, port)
    try:
        filepath = client.read_file(filename)
    finally:
        transport.close()

    dprint(f"Pretend you're reading a file: {filename} at {filepath}")
    dprint(f"Done reading file")

def write_file(ip, port, filename, filepath):
    dprint(f"Writing File: {filename} at path: {filepath}")
    client, transport = open_client(ip, port)

    try:
        client.write_file(filename, filepath)
    finally:
        transport.close()

#  ███╗   ███╗ █████╗ ██╗███╗   ██╗
#  ████╗ ████║██╔══██╗██║████╗  ██║
#  ██╔████╔██║███████║██║██╔██╗ ██║
#  ██║╚██╔╝██║██╔══██║██║██║╚██╗██║
#  ██║ ╚═╝ ██║██║  ██║██║██║ ╚████║
#  ╚═╝     ╚═╝╚═╝  ╚═╝╚═╝╚═╝  ╚═══╝

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

    #TODO: Figure out a way to get the filename from the filepath for write operations then you only need one entry

    if args.list:
        list_files(args.server_ip, args.server_port)

    elif args.read:
        read_file(args.server_ip, args.server_port, args.read)

    elif args.write:
        write_file(args.server_ip, args.server_port, args.write[0], args.write[1])
    
if __name__ == "__main__":
    main()
