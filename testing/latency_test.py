import subprocess
import argparse
import statistics
import redis
import os
import shutil
import signal
import logging

from timeit import default_timer as timer

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
    process = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    
def start_service(cmd: str) -> int:
    return subprocess.Popen(cmd.split()).pid

def do_append(ms_addr: str, op_size: int) -> float:
    start = timer()
    cmd = f'./client -ms={ms_addr} -ops=append -filename={filename} -size={op_size * 1024 * 1024} -source=../client/data/{op_size}_mb.txt'
    run_command(cmd)
    end = timer()
    return end - start


def do_read(ms_addr: str, op_size: int) -> float:
    pass

def do_create(ms_addr: str, filename: str):
    cmd = f'./client -ms={ms_addr} -ops=create -filename={filename}'
    run_command(cmd)
    
    
def setup(redis_port: int, num_chunkservers: int) -> Config:
    # connect to redis and clear state
    logging.info('clearing redis states')
    r = redis.Redis(
        host='localhost',
        port=redis_port,
        password=''
    )
    r.delete('files')
    
    logging.info('booting master server')
    starting_port = 10000
    ms_pid = start_service(f'../masterserver/cmd/main')
    
    logging.info('booting chunk servers')
    cs_pids = []
    for i in range(num_chunkservers):
        logging.info(f'booting chunk server #{i+1}')
        
        cs_pid = start_service(f'../chunkserver/cmd/main --host localhost --port {starting_port + i} --path ~/CDFS{i+1} --mhost localhost --mport 8080 --hb 200')
        cs_pids.append(cs_pid)
        
    return Config(ms_pid, cs_pids)


def cleanup(config: Config):
    os.kill(config.ms_pid, signal.SIGTERM)
    for i in range(len(config.cs_pids)):
        cs_pid = config.cs_pids[i]
        os.kill(cs_pid, signal.SIGTERM)
        remove_all(f'~/CSFS{i+1}')
        
        
    logging.info('killed master server and chunk servers')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='CDFS latency test')
    parser.add_argument('--ms', type=str, help='addr of master server')
    parser.add_argument('--op', type=str, choices=['append', 'read'], help='mode of operation (append or read)')
    parser.add_argument('--size', type=int, help='append or read size in MB')
    parser.add_argument('--iter', type=int, help='#iterations')
    parser.add_argument('--redis-port', type=int)
    parser.add_argument('--num_cs', type=int)
    
    args = parser.parse_args()
    
    operation = args.op
    operation_size = args.size
    ms_addr = args.ms
    op_iter = args.iter
    filename = 'file1'
    redis_port = args.redis_port
    num_cs = args.num_cs
    
    config = setup(redis_port, num_cs)
    
    data_points = []
    
    
    if operation.lower().strip() == 'append':
        do_create(ms_addr, filename)
        for _ in range(op_iter):
            rtt = do_append(ms_addr, operation_size)
            data_points.append(rtt)
            pass
        pass
    elif operation.lower().strip() == 'read':
        pass


    avg = statistics.mean(data_points)
    med = statistics.median(data_points)
    stddev = statistics.stdev(data_points)
    print(f'result: avg={avg}, med={med}, stdev={stddev}')    
    
    cleanup(config)
