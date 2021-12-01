import subprocess
from concurrent.futures import ProcessPoolExecutor


def ms_worker_instance(host, port):
    process = subprocess.Popen('python microservice_creator.py -ms_host="{}" -ms_port={} -ms_log_level={}'
                               .format(host, port, 'warning'),
                               shell=True,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT,
                               encoding='utf-8',
                               errors='replace')
    try:
        while True:
            realtime_output = process.stdout.readline()
            if realtime_output == '' and process.poll() is not None:
                break
            if realtime_output:
                print("{host}:{port} - ".format(host=host, port=port) + realtime_output.strip(), flush=True)
    finally:
        process.kill()
        print("{host}:{port} - ".format(host=host, port=port) + ' is terminated.', flush=True)


if __name__ == '__main__':
    num_of_node = 6
    list_instance = [('127.0.0.{}'.format(i), 8000) for i in range(2, num_of_node + 1 + 2 - 1)]

    with ProcessPoolExecutor(max_workers=len(list_instance)) as Executor:
        for host, port in list_instance:
            Executor.submit(ms_worker_instance, host, port)