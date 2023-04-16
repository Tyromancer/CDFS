import subprocess
import argparse
import statistics
import redis
import os
import shutil
import signal
import logging
import time
import concurrent.futures
import queue
import threading

from timeit import default_timer as timer
from itertools import chain

class Config:
    def __init__(self, ms_pid, cs_pids):
        self.ms_pid: int = ms_pid
        self.cs_pids: list[int] = cs_pids
        
def remove_all(folder: str):
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))

def run_command(cmd: str):
    process = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, shell=False)
    process.wait()
    # output, error = process.communicate()
    
def start_service(cmd: str) -> int:
    return subprocess.Popen(cmd.split()).pid

def do_append(ms_addr: str, filename: str, op_size: int) -> float:
    start = timer()
    cmd = f'./client -ms={ms_addr} -ops=append -filename={filename} -size={op_size * 1024 * 1024} -source=../client/data/{op_size}_mb.txt'
    run_command(cmd)
    end = timer()
    return end - start


def do_read(ms_addr: str, filename: str, op_size: int, target_path: str) -> float:
    start = timer()
    cmd = f'./client -ms={ms_addr} -ops=read -filename={filename} -size={op_size * 1024 * 1024} -offset=0 -target={target_path}'
    run_command(cmd)
    end = timer()
    return end-start

def do_create(ms_addr: str, filename: str):
    cmd = f'./client -ms={ms_addr} -ops=create -filename={filename}'
    run_command(cmd)
    
    
def setup(redis_port: int, num_chunkservers: int, path: str) -> Config:
    # connect to redis and clear state
    logging.info('clearing redis states')
    r = redis.Redis(
        host='localhost',
        port=redis_port,
        password=''
    )
    r.delete('files')
    
    logging.info('booting master server')
    starting_port = 12345
    ms_pid = start_service(f'../masterserver/cmd/main')
    time.sleep(2)
    
    
    logging.info('booting chunk servers')
    cs_pids = []
    for i in range(num_chunkservers):
        logging.info(f'booting chunk server #{i+1}')
        
        cs_pid = start_service(f'../chunkserver/cmd/main --host localhost --port {starting_port + i} --path {path}/CDFS{i+1} --mhost localhost --mport 8080 --hb 200')
        cs_pids.append(cs_pid)
        
    return Config(ms_pid, cs_pids)


def cleanup(config: Config, path: str):
    os.kill(config.ms_pid, signal.SIGTERM)
    for i in range(len(config.cs_pids)):
        cs_pid = config.cs_pids[i]
        os.kill(cs_pid, signal.SIGTERM)
        try:
            remove_all(f'{path}/CDFS{i+1}')
        except FileNotFoundError as e:
            logging.warning(e)
        
    remove_all('client_output')
    logging.info('killed master server and chunk servers')
    
    
def client_worker(ms_addr: str, filename: str, operation: str, operation_size: int, op_iter: int, q: queue.Queue):
    print(f'starting client with operation {operation}')
    data_points = []
    
    for i in range(op_iter):
        if operation.lower().strip() == 'append':
            rtt = do_append(ms_addr, filename, operation_size)
            # print(f'rtt is {rtt}')
            data_points.append(rtt)
        elif operation.lower().strip() == 'read':
            rtt = do_read(ms_addr,filename, operation_size,f'./client_output/output_{filename}_{i}.txt')
            data_points.append(rtt)            

    q.put(data_points)
    
def get_size(start_path='.'):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            # skip if it is symbolic link
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)

    return total_size

if __name__ == '__main__':
    logging.basicConfig()
    parser = argparse.ArgumentParser(description='CDFS latency test')
    parser.add_argument('--ms', type=str, help='addr of master server')
    parser.add_argument('--op', type=str, choices=['append', 'read', 'lb'], help='mode of operation (append, read, lb)')
    parser.add_argument('--size', type=int, help='append or read size in MB')
    parser.add_argument('--iter', type=int, help='#iterations')
    parser.add_argument('--redis-port', type=int)
    parser.add_argument('--num_cs', type=int)
    parser.add_argument('--path', type=str)
    parser.add_argument('--num_client', type=int)
    
    args = parser.parse_args()
    
    operation = args.op
    operation_size = args.size
    ms_addr = args.ms
    op_iter = args.iter
    filename = 'file'
    redis_port = args.redis_port
    num_cs = args.num_cs
    path = args.path
    num_client = args.num_client
    
    config = setup(redis_port, num_cs, path)
    time.sleep(2)
    data_points = []
    
    q = queue.Queue()
    threads = []
    # do_create(ms_addr, filename)
    for i in range(num_client):
        do_create(ms_addr, f'{filename}{i+1}')
    time.sleep(1)
    start_total = None
    end_total = None
    if operation.lower().strip() == 'append' or operation.lower().strip() == 'lb':
        start_total = timer()
        for i in range(num_client):
            t = threading.Thread(target=client_worker, name=f'thread{i}', args=[ms_addr, f'{filename}{i+1}', 'append', operation_size, op_iter, q])
            threads.append(t)
            t.start()
        
        
    elif operation.lower().strip() == 'read':
        for i in range(num_client):
            do_append(ms_addr,f'{filename}{i+1}',operation_size)
        
        start_total = timer()
        for i in range(num_client):
            t = threading.Thread(target=client_worker,name=f'thread{i}', args=[ms_addr, f'{filename}{i+1}', operation, operation_size, op_iter, q])
            threads.append(t)
            t.start()

    for i in range(len(threads)):
        t = threads[i]
        t.join()
        pts = q.get()
        #print(f'pts is {pts}')
        data_points.append(pts)
    end_total = timer()
    total_time = end_total - start_total 

    data_points = list(chain(*data_points))


    avg = statistics.mean(data_points)
    med = statistics.median(data_points)
    stddev = statistics.stdev(data_points)
    
    # check load balance
    if operation.lower().strip() == 'lb':
        directory_sizes = []
        for i in range(num_cs):
            d_dir = os.path.join(os.getcwd(), f'CDFS{i+1}')
            print(f'd_dir is {d_dir}')
            if os.path.isdir(d_dir):
                # d_size = sum(os.path.getsize(f) for f in os.listdir(d_dir) if os.path.isfile(f))
                d_size = get_size(d_dir) / (1024 * 1024)
                print(f'size of CDFS{i+1} is {d_size}')
                directory_sizes.append(d_size)
        print(f'lb result: stdev is {statistics.stdev(directory_sizes)}')

    
    
    print(f'result: avg={avg}, med={med}, stdev={stddev}, total time = {total_time}')    
    
    cleanup(config, path)
