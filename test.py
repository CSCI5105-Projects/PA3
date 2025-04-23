# test.py
# Written by Matthew Breach and Lily Hymes

"""
Testing script for PA3
---------------------------------------
* Launches multiple replica servers with varying quorum parameters (NR, NW).
* Spawns concurrent read and write clients to measure runtime under different workloads.
* Aggregates and records results in a CSV file (`results.csv` by default).
* Generates PNG heatmaps of read/write times:
    - Averaged across all runs (saved to `heatmaps/read_heatmap_avg.png` and `heatmaps/write_heatmap_avg.png`)
    - For each unique (nodes, clients) configuration (saved to `heatmaps/nX_cY_*.png`)
* Supports optional CSV import for visualization-only runs.
* Provides a cleanup option to delete all test data and heatmaps.

CSV schema:
    nodes, NR, NW, clients, read_s, write_s

Usage: 
    python3 test.py [-h] [--plot] [--csv CSV] [--clean] [max_nodes] [max_clients]

    Test script for CSCI 5105 PA3

    Positional arguments:
        max_nodes    maximum number of replicas to launch
        max_clients  maximum concurrent clients

    Optional arguments:
        -h, --help   show this help message and exit
        --plot       produce heat‑map PNGs
        --csv CSV    skip experiments and load an existing results CSV
        --clean      remove heatmaps/ and pa3_test/ then exits
"""

#  ██████╗ ██████╗ ███╗   ██╗███████╗██╗ ██████╗ 
# ██╔════╝██╔═══██╗████╗  ██║██╔════╝██║██╔════╝ 
# ██║     ██║   ██║██╔██╗ ██║█████╗  ██║██║  ███╗
# ██║     ██║   ██║██║╚██╗██║██╔══╝  ██║██║   ██║
# ╚██████╗╚██████╔╝██║ ╚████║██║     ██║╚██████╔╝
#  ╚═════╝ ╚═════╝ ╚═╝  ╚═══╝╚═╝     ╚═╝ ╚═════╝                          

import subprocess, time, os, shutil, socket, argparse, csv, uuid, threading, contextlib
from collections import defaultdict
import matplotlib.pyplot as plt
import numpy as np

REPLICA_SCRIPT = "replica_server.py"
CLIENT_SCRIPT  = "client.py"
HOST           = "127.0.0.1"
TEST_DIR       = "pa3_test"
RESULTS_FILE   = "results.csv"  # default output when we run experiments
TEST_FILE      = "chicken_jockey.jpg"
HEATMAP_DIR    = "heatmaps"
AVG_READ_PNG   = os.path.join(HEATMAP_DIR, "read_heatmap_avg.png")
AVG_WRITE_PNG  = os.path.join(HEATMAP_DIR, "write_heatmap_avg.png")
QUORUM_DELAY   = 1.0
SHUTDOWN_WAIT  = 2.0

# ██╗  ██╗███████╗██╗     ██████╗ ███████╗██████╗ ███████╗
# ██║  ██║██╔════╝██║     ██╔══██╗██╔════╝██╔══██╗██╔════╝
# ███████║█████╗  ██║     ██████╔╝█████╗  ██████╔╝███████╗
# ██╔══██║██╔══╝  ██║     ██╔═══╝ ██╔══╝  ██╔══██╗╚════██║
# ██║  ██║███████╗███████╗██║     ███████╗██║  ██║███████║
# ╚═╝  ╚═╝╚══════╝╚══════╝╚═╝     ╚══════╝╚═╝  ╚═╝╚══════╝

# Ensure testing file (chicken_jockey.jpg) exists                                     
def ensure_test_file():
    if not os.path.isfile(TEST_FILE):
        raise FileNotFoundError(f"Test JPEG '{TEST_FILE}' not found")

# Dynamically finds free port on system
def find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]

# Computes all NR, NW pairs for number of replica servers
def compute_valid_quorums(N):
    return [(r, w) for r in range(1, N + 1) for w in range(1, N + 1)
            if r + w > N and w > N / 2]

# Update compute_nodes.txt for the localhost given a list of ports and NR, NW
def write_compute_nodes(ports, NR, NW):
    with open("compute_nodes.txt", "w") as fp:
        fp.write(f"{NR},{NW}\n")
        for i, p in enumerate(ports):
            flag = 1 if i == 0 else 0
            fp.write(f"{HOST},{p},{flag}\n")

# Launch replica servers in list ports on localhost
def launch_replicas(ports):
    shutil.rmtree(TEST_DIR, ignore_errors=True)
    os.makedirs(TEST_DIR)
    procs = []
    for idx, port in enumerate(ports):
        storage = os.path.join(TEST_DIR, f"node_{idx}")
        os.makedirs(storage, exist_ok=True)
        procs.append(subprocess.Popen(["python3", REPLICA_SCRIPT, HOST, str(port), storage, "-d"]))
    return procs

# Run client process with cmd args cmd
def run_client(cmd):
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if proc.returncode != 0:
        print("CLIENT ERROR:\n", proc.stderr)
    return proc.stdout.strip()

# Gracefully shutdown a list of processes
def shutdown(procs):
    for p in procs: p.terminate()
    time.sleep(SHUTDOWN_WAIT)
    for p in procs:
        if p.poll() is None:
            p.kill()
    for p in procs: p.wait()

# Write data to csv file
def record_csv(headers, rows, path):
    with open(path, "w", newline="") as f:
        csv.writer(f).writerows([headers, *rows])

#  ████████╗███████╗███████╗████████╗
#  ╚══██╔══╝██╔════╝██╔════╝╚══██╔══╝
#     ██║   █████╗  ███████╗   ██║   
#     ██║   ██╔══╝  ╚════██║   ██║   
#     ██║   ███████╗███████║   ██║   
#     ╚═╝   ╚══════╝╚══════╝   ╚═╝   

# Preliminary write for the read test
def seed_object(port):
    run_client(["python3", CLIENT_SCRIPT, HOST, str(port), "--write", TEST_FILE, TEST_FILE])
    time.sleep(0.3) # Wait to ensure file is written to the servers

# Use threading to run concurrent read/write tests for the number of clients n_clients
def concurrent_test(ports, n_clients, workload):
    if workload not in {"read", "write"}:
        raise ValueError("workload must be 'read' or 'write'")

    if workload == "read": # Ensure file exists for read test
        seed_object(ports[0])

    results = [None] * n_clients
    temp_files = []

    # Worker function to read/write to target server
    def worker(i):
        target = ports[i % len(ports)]
        if workload == "read":
            cmd = ["python3", CLIENT_SCRIPT, HOST, str(target), "--read", TEST_FILE]
        else:
            tmp = f"tmp_{uuid.uuid4()}.jpg"
            shutil.copy(TEST_FILE, tmp)
            temp_files.append(tmp)
            cmd = ["python3", CLIENT_SCRIPT, HOST, str(target), "--write", tmp, tmp]
        results[i] = run_client(cmd)

    # Spool up clients and track runtime
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_clients)]
    start = time.time()
    for t in threads: t.start()
    for t in threads: t.join()
    elapsed = time.time() - start

    # Cleanup temporary files
    for f in temp_files:
        try:
            os.remove(f)
        except FileNotFoundError:
            pass
    
    # Return time elapsed for current run
    return elapsed

#  ██████╗ ██╗      ██████╗ ████████╗████████╗██╗███╗   ██╗ ██████╗ 
#  ██╔══██╗██║     ██╔═══██╗╚══██╔══╝╚══██╔══╝██║████╗  ██║██╔════╝ 
#  ██████╔╝██║     ██║   ██║   ██║      ██║   ██║██╔██╗ ██║██║  ███╗
#  ██╔═══╝ ██║     ██║   ██║   ██║      ██║   ██║██║╚██╗██║██║   ██║
#  ██║     ███████╗╚██████╔╝   ██║      ██║   ██║██║ ╚████║╚██████╔╝
#  ╚═╝     ╚══════╝ ╚═════╝    ╚═╝      ╚═╝   ╚═╝╚═╝  ╚═══╝ ╚═════╝

# Save heatmap for given data                      
def save_heatmap(triples, xlabel, ylabel, title, fname):
    xs = sorted({x for x, _, _ in triples})
    ys = sorted({y for _, y, _ in triples})
    mat = np.full((len(ys), len(xs)), np.nan)
    for x, y, t in triples:
        mat[ys.index(y), xs.index(x)] = t

    plt.figure()
    im = plt.imshow(mat, aspect="auto", origin="lower")
    plt.colorbar(im, label="Time (s)")
    plt.xticks(range(len(xs)), xs)
    plt.yticks(range(len(ys)), ys)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.savefig(fname)
    plt.close()

# Generate all heatmaps for all data
def generate_all_heatmaps(results, plot_flag):
    if not plot_flag:
        return

    # Ensure directory to save heatmaps exists
    os.makedirs(HEATMAP_DIR, exist_ok=True)

    # Generate heatmaps for averaged read and write data over all servers and clients
    agg_read, agg_write = defaultdict(list), defaultdict(list)
    for n, NR, NW, c, read_t, write_t in results:
        agg_read[(NR, NW)].append(read_t)
        agg_write[(NR, NW)].append(write_t)

    avg_read = [(NR, NW, sum(ts)/len(ts)) for (NR, NW), ts in agg_read.items()]
    avg_write = [(NR, NW, sum(ts)/len(ts)) for (NR, NW), ts in agg_write.items()]

    save_heatmap(avg_read,  "NR", "NW", "Average Read Times",  AVG_READ_PNG)
    save_heatmap(avg_write, "NR", "NW", "Average Write Times", AVG_WRITE_PNG)

    # Generate per-trial heatmaps
    combos = {(r[0], r[3]) for r in results}
    for n, c in combos:
        subset_read  = [(NR, NW, read_t)  for n2, NR, NW, c2, read_t, _      in results if n2 == n and c2 == c]
        subset_write = [(NR, NW, write_t) for n2, NR, NW, c2, _,      write_t in results if n2 == n and c2 == c]
        if not subset_read:
            continue  # should not happen
        base = os.path.join(HEATMAP_DIR, f"n{n}_c{c}")
        save_heatmap(subset_read,  "NR", "NW", f"Read Times (n={n}, c={c})",  f"{base}_read.png")
        save_heatmap(subset_write, "NR", "NW", f"Write Times (n={n}, c={c})", f"{base}_write.png")


#   ██████╗██╗     ███████╗ █████╗ ███╗   ██╗██╗   ██╗██████╗ 
#  ██╔════╝██║     ██╔════╝██╔══██╗████╗  ██║██║   ██║██╔══██╗
#  ██║     ██║     █████╗  ███████║██╔██╗ ██║██║   ██║██████╔╝
#  ██║     ██║     ██╔══╝  ██╔══██║██║╚██╗██║██║   ██║██╔═══╝ 
#  ╚██████╗███████╗███████╗██║  ██║██║ ╚████║╚██████╔╝██║     
#   ╚═════╝╚══════╝╚══════╝╚═╝  ╚═╝╚═╝  ╚═══╝ ╚═════╝ ╚═╝     

def clean_artifacts():
    # Remove heatmaps folder & standalone avg maps
    remove_heatmap_dir()

    # Remove results file
    with contextlib.suppress(FileNotFoundError):
        os.remove("results.csv")

    # Remove all node storage dirs
    remove_test_dir()
    print("Cleaned testing artifacts.")

# Remove heatmap directory
def remove_heatmap_dir():
    shutil.rmtree(HEATMAP_DIR, ignore_errors=True)

# Remove test dir
def remove_test_dir():
    shutil.rmtree(TEST_DIR, ignore_errors=True)

#  ███╗   ███╗ █████╗ ██╗███╗   ██╗
#  ████╗ ████║██╔══██╗██║████╗  ██║
#  ██╔████╔██║███████║██║██╔██╗ ██║
#  ██║╚██╔╝██║██╔══██║██║██║╚██╗██║
#  ██║ ╚═╝ ██║██║  ██║██║██║ ╚████║
#  ╚═╝     ╚═╝╚═╝  ╚═╝╚═╝╚═╝  ╚═══╝

def main():
    # Verify test file exists
    ensure_test_file()

    # Parse args
    ap = argparse.ArgumentParser(description="Test script for CSCI 5105 PA3")
    ap.add_argument("max_nodes", type=int, nargs="?", help="maximum number of replicas to launch")
    ap.add_argument("max_clients", type=int, nargs="?", help="maximum concurrent clients")
    ap.add_argument("--plot", action="store_true", help="produce heat‑map PNGs")
    ap.add_argument("--csv", type=str, help="skip experiments and load an existing results CSV")
    ap.add_argument("--clean", action="store_true", help="remove heatmaps/ and pa3_test/ then exits")
    args = ap.parse_args()

    # Clean testing artifacts
    if args.clean:
        print("WARNING: This will permanently delete all generated heat‑maps and the pa3_test/ directory.")
        reply = input("Continue? [Y/N]: ").strip().lower()
        if reply == "y":
            clean_artifacts()
        else:
            print("Aborted cleanup.")
        exit(0)

    results = []  # each row: [nodes, NR, NW, clients, read_s, write_s]

    # Load/generate data
    if args.csv:
        with open(args.csv, newline="") as f:
            reader = csv.reader(f)
            headers = next(reader) # Strip headers
            for row in reader:
                nodes, NR, NW, clients, read_s, write_s = row
                results.append([int(nodes), int(NR), int(NW), int(clients), float(read_s), float(write_s)])
        print(f"Loaded {len(results)} rows from {args.csv}")
    else:
        # Validate args
        if args.max_nodes is None or args.max_clients is None:
            raise SystemExit("max_nodes and max_clients are required when not using --csv")
        if args.max_nodes < 7 or args.max_clients < 3:
            raise SystemExit("Need ≥7 nodes and ≥3 client")

        # Loop through replica servers
        for n in range(7, args.max_nodes + 1):
            ports = [find_free_port() for _ in range(n)]
            for NR, NW in compute_valid_quorums(n):
                write_compute_nodes(ports, NR, NW)
                procs = launch_replicas(ports)
                time.sleep(QUORUM_DELAY)

                # Loop through clients
                for c in range(3, args.max_clients + 1):
                    r_t = concurrent_test(ports, c, "read")
                    w_t = concurrent_test(ports, c, "write")
                    print(f"n={n}, NR={NR}, NW={NW}, clients={c}, read={r_t:.2f}s, write={w_t:.2f}s")
                    results.append([n, NR, NW, c, round(r_t, 2), round(w_t, 2)])

                # Cleanup processes
                shutdown(procs)

        record_csv(["nodes", "NR", "NW", "clients", "read_s", "write_s"], results, RESULTS_FILE)
        print(f"Wrote {RESULTS_FILE}")

    # Cleanup temp dirs
    remove_test_dir()

    # Generate heatmaps
    generate_all_heatmaps(results, args.plot)

if __name__ == "__main__":
    main()
