#!/usr/bin/env python3
import subprocess
import time
import os
import shutil
import socket
import argparse
import signal
import csv
import threading
import matplotlib.pyplot as plt

# ==== CONFIGURATION ====
REPLICA_SCRIPT = "replica_server.py"
CLIENT_SCRIPT  = "client.py"
HOST           = "127.0.0.1"
QUORUM_DELAY    = 1.0        # seconds to wait after launching servers
TEST_DIR        = "pa3_test" # where we'll spin up storage dirs
SHUTDOWN_WAIT   = 2.0        # seconds to wait for clean shutdown
RESULTS_FILE    = "results.csv"
# =======================

def find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def compute_valid_quorums(N):
    """
    Return all (NR,NW) pairs satisfying NR + NW > N and NW > N/2
    """
    quorums = []
    for NR in range(1, N+1):
        for NW in range(1, N+1):
            if NR + NW > N and NW > N/2:
                quorums.append((NR, NW))
    return quorums


def write_compute_nodes(ports, NR, NW):
    N = len(ports)
    with open("compute_nodes.txt", "w") as f:
        f.write(f"{NR},{NW}\n")
        for i, p in enumerate(ports):
            role = 1 if i == 0 else 0
            f.write(f"{HOST},{p},{role}\n")
    print(f"→ configured NR={NR}, NW={NW} for N={N}")


def launch_replicas(ports):
    procs = []
    # clean test directory
    if os.path.isdir(TEST_DIR): shutil.rmtree(TEST_DIR)
    os.makedirs(TEST_DIR)
    for i, p in enumerate(ports):
        storage = os.path.join(TEST_DIR, f"node_{i}")
        os.makedirs(storage, exist_ok=True)
        cmd = ["python3", REPLICA_SCRIPT, HOST, str(p), storage, "-d"]
        print(f"Launching {len(ports)} replicas: node {i}@{p}")
        procs.append(subprocess.Popen(cmd))
    return procs


def run_client(cmd, results, idx):
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    if p.returncode != 0:
        print(f"CLIENT ERROR: {p.stderr}")
    results[idx] = p.stdout.strip()


def smoke_test(ports):
    # basic smoke test for single quorum
    if len(ports) < 3:
        return
    # ensure hello.txt exists
    with open("hello.txt", "w") as f:
        f.write("hello,PA3!\n")
    # write via first replica
    run_client(["python3", CLIENT_SCRIPT, HOST, str(ports[0]), "--write", "hello.txt", "hello.txt"], {}, 0)
    time.sleep(0.2)


def concurrent_test(ports, num_clients, workload='read'):
    threads = []
    results = [None] * num_clients
    # prepare for reads
    if workload == 'read':
        with open("hello.txt", "w") as f:
            f.write("hello,PA3!\n")
        run_client(["python3", CLIENT_SCRIPT, HOST, str(ports[0]), "--write", "hello.txt", "hello.txt"], {}, 0)
        time.sleep(0.2)

    for i in range(num_clients):
        target = ports[i % len(ports)]
        if workload == 'read':
            cmd = ["python3", CLIENT_SCRIPT, HOST, str(target), "--read", "hello.txt"]
        else:
            # unique file per client
            fname = f"file_{i}.txt"
            with open(fname, 'w') as f:
                f.write(f"data {i}\n")
            cmd = ["python3", CLIENT_SCRIPT, HOST, str(target), "--write", fname, fname]
        t = threading.Thread(target=run_client, args=(cmd, results, i))
        threads.append(t)

    start = time.time()
    for t in threads: t.start()
    for t in threads: t.join()
    return time.time() - start


def shutdown_procs(procs):
    for p in procs: p.terminate()
    time.sleep(SHUTDOWN_WAIT)
    for p in procs:
        if p.poll() is None:
            p.kill()
    for p in procs: p.wait()


def record_results_csv(fieldnames, rows):
    with open(RESULTS_FILE, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(fieldnames)
        w.writerows(rows)


def plot_heatmap(data, x_label, y_label, title, fname):
    import numpy as np
    xs = sorted({r[0] for r in data})
    ys = sorted({r[1] for r in data})
    mat = np.zeros((len(ys), len(xs)))
    for x, y, t in data:
        mat[ys.index(y), xs.index(x)] = t

    plt.figure()
    plt.imshow(mat, aspect='auto', origin='lower')
    plt.colorbar(label='Time(s)')
    plt.xticks(range(len(xs)), xs)
    plt.yticks(range(len(ys)), ys)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(title)
    plt.savefig(fname)
    print(f"Saved heatmap {fname}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('max_nodes', type=int)
    parser.add_argument('max_clients', type=int)
    parser.add_argument('--plot', action='store_true')
    args = parser.parse_args()

    if args.max_nodes < 3 or args.max_clients < 1:
        print('Need >=3 nodes and >=1 clients')
        return

    results = []  # [nodes, NR, NW, clients, read_s, write_s]

    for n in range(3, args.max_nodes + 1):
        ports = [find_free_port() for _ in range(n)]
        # try every valid quorum config
        for NR, NW in compute_valid_quorums(n):
            write_compute_nodes(ports, NR, NW)
            procs = launch_replicas(ports)
            time.sleep(QUORUM_DELAY)

            for c in range(1, args.max_clients + 1):
                rt = concurrent_test(ports, c, 'read')
                wt = concurrent_test(ports, c, 'write')
                print(f"n={n}, NR={NR}, NW={NW}, clients={c}, read={rt:.2f}s, write={wt:.2f}s")
                results.append([n, NR, NW, c, round(rt, 2), round(wt, 2)])

            shutdown_procs(procs)

    # write out CSV
    headers = ['nodes', 'NR', 'NW', 'clients', 'read_s', 'write_s']
    record_results_csv(headers, results)

    if args.plot:
        # heatmap of best read time per quorum size (example using NR,NW)
        # filter for a fixed client count or average as needed
        plot_heatmap([(r[1], r[2], r[4]) for r in results], 'NR', 'NW', 'Read Times', 'read_heatmap.png')
        plot_heatmap([(r[1], r[2], r[5]) for r in results], 'NR', 'NW', 'Write Times', 'write_heatmap.png')


if __name__ == '__main__':
    main()
