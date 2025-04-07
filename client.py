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
from PA3.ttypes import FileInfo

from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol

import argparse



def main():
    parser = argparse.ArgumentParser(description="Client for reading and writing")
    parser.add_argument("server_ip", type=str, help="Server IP address")
    parser.add_argument("server_port", type=int, help="Server port")
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debug output")

    args = parser.parse_args()
    if args.debug:
        DEBUG = 1


if __name__ == "__main__":
    main()