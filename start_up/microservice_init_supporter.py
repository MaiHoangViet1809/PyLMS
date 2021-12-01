import subprocess
import time
from concurrent.futures import ProcessPoolExecutor


def ms_support_instance(filename, delay_time: int = 0):
    time.sleep(delay_time)
    process = None
    try:
        process = subprocess.Popen('python {}.py'.format(filename),
                                   shell=True,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT,
                                   encoding='utf-8',
                                   errors='replace')
        while True:
            realtime_output = process.stdout.readline()
            if realtime_output == '' and process.poll() is not None:
                break
            if realtime_output:
                print("{} - ".format(filename) + realtime_output.strip(), flush=True)
    finally:
        process.kill()
        print("{} - ".format(filename) + ' is terminated.', flush=True)


if __name__ == '__main__':
    list_instance = [('ms_support_api_registry', 0),
                     ('ms_support_kafka_msg', 3),
                     ('ms_support_log_interface', 6)]

    with ProcessPoolExecutor(max_workers=len(list_instance)) as Executor:
        for file_name, delay in list_instance:
            Executor.submit(ms_support_instance, file_name, delay)