# CSCI 5105 Distributed Systems PA3
**Authors:** Matthew Breach ([breac001](mailto:breac001@umn.edu)) and Lily Hymes ([hymes019](mailto:hymes019@umn.edu))

Project is written in python, therefore compilation is not required

## 1 Assumptions

1. User has a valid python installation
   
2. Thrift files are required for operation they should be located at
   - `../thrift/thrift-0.19.0/lib/py/build/lib*`
  
3. Thrift generated files are required for operation they should be located at
   - `/gen-py/PA2/`

5. If further print statements are desired, add a -d flag to the arguments

---

## 2 `compute_nodes.txt` format
```
NR,NW               ← first line = quorum sizes
ip0,port0,1         ← coordinator (flag = 1)
ip1,port1,0         ← replica (flag = 0)
…
```
*Exactly one* replica must carry the coordinator flag.

---

## 3 Running a single replica‐server
```bash
python3 replica_server.py <ip> <port> <storage_dir> [-d]
```
*Example*
```bash
python3 replica_server.py 127.0.0.1 9090 node0 -d
```
`-d` enables verbose debug output.

---

## 4 Client usage
```bash
python3 client.py <server_ip> <server_port> [options]

Options:
  -l, --list                     list all files & versions in the DFS
  -r FILE, --read FILE           read FILE into the contacted replica
  -w FILE PATH, --write FILE PATH  write local PATH to DFS under name FILE
  -d, --debug                    enable debug output
```
*Examples*
```bash
python3 client.py 127.0.0.1 9090 --list
python3 client.py 127.0.0.1 9090 --read report.pdf
python3 client.py 127.0.0.1 9090 --write cat.png ./cat.png
```

---

## 5 Automated benchmarking (`test.py`)
```bash
python3 test.py <max_nodes> <max_clients> [--plot] [--csv FILE] [--clean]
```
*Typical full sweep*
```bash
python3 test.py 10 10 --plot
```
This launches 7–10 replicas, 3–10 clients, tests **all** valid `(NR,NW)` pairs, produces `results.csv`, and saves heat‑maps under `heatmaps/`.

*Re‑plot from existing data*
```bash
python3 test.py --csv results.csv --plot
```

*Remove all generated artefacts*
```bash
python3 test.py --clean
```
`--clean` asks for confirmation, then deletes `heatmaps/`, `results.csv`, and the temporary `pa3_test/` directory.

---

## 6 Repository layout
```
PA3.thrift           ← IDL
replica_server.py    ← replica implementation & coordinator logic
client.py            ← CLI client
compute_nodes.txt    ← example topology (generated automatically by test.py)
test.py              ← benchmarking & visualisation script
README.md            ← this file
PA3 Design Document.pdf
gen-py               ← Thrift files
heatmaps             ← Heatmaps from test with 10 servers and 10 clients
results.csv          ← Timing results from test with 10 servers and 10 clients
```

---

## 7 Troubleshooting
* Use the `-d` flag on both `replica_server.py` and `client.py` to enable detailed logging.

---

## 8 Acknowledgements
Small helper snippets (port discovery, CLI arg handling, ASCII banners) were adapted from ChatGPT suggestions or online websites.

ASCII banners generated using: https://patorjk.com/software/taag/#p=display&h=0&v=0&c=bash&f=ANSI%20Shadow&t=EXAMPLE%0A

