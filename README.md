# CSCI 5105 Distributed Systems PA3
Matthew Breach ([breac001](mailto:breac001@umn.edu)) and Lily Hymes ([hymes019](mailto:hymes019@umn.edu))

Project is written in python, therefore compilation is not required

## Assumptions

1. User has a valid python installation
   
2. Thrift files are required for operation they should be located at
   - `../thrift/thrift-0.19.0/lib/py/build/lib*`
  
3. Thrift generated files are required for operation they should be located at
   - `/gen-py/PA2/`

5. If further print statements are desired, add a -d flag to the arguments

## Execution Arguments

1. replica_server.py
   - `python3 replica_server.py <server_ip> <server_port> <storage_location>`

2. client.py
   - `python3 client.py <server_ip> <server_port> <task> <arg1> <arg2>`
   - e.g. `python3 client.py 127.0.0.1 9090 --list`
   - e.g. `python3 client.py 127.0.0.1 9090 --read skibidi.txt`
   - e.g. `python3 client.py 127.0.0.1 9090 --write skibidi.txt ./skibidi.txt`
