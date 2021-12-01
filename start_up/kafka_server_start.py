import os
import sys
import time
import inspect
import subprocess
from concurrent.futures import ProcessPoolExecutor


def get_os_var(var_name):
    try:
        var1 = os.environ[var_name]
    except KeyError:
        var1 = None
        print(inspect.stack()[0][3], "System-Variables {} is not exist!!".format(var_name))
    return var1


def set_os_var(var_name, var_value):
    # noinspection PyBroadException
    try:
        os.environ[var_name] = var_value
        return True
    except:
        print(inspect.stack()[0][3], "Unexpected error: {}".format(sys.exc_info()[0]))
        return False


KAFKA_HOME = "D:/Kafka/kafka_2.13-2.8.1"
JAVA_HOME = "C:/Program Files/Java/jre1.8.0_281"

if get_os_var("KAFKA_HOME") is None:
    set_os_var("KAFKA_HOME", KAFKA_HOME)

if get_os_var("JAVA_HOME") is None:
    set_os_var("JAVA_HOME", JAVA_HOME)


def start_ZooKeeper():
    print("Start process for ZooKeeper")
    p_zookeeper = subprocess.Popen(KAFKA_HOME + '/start_up/windows/zookeeper-server-start.bat ./config/zookeeper.properties'
                                   , shell=True
                                   , stdout=subprocess.PIPE
                                   , stderr=subprocess.STDOUT
                                   , cwd=KAFKA_HOME
                                   , encoding='utf-8'
                                   , errors='replace'
                                   )
    while True:
        realtime_output = p_zookeeper.stdout.readline()
        if realtime_output == '' and p_zookeeper.poll() is not None:
            break
        if realtime_output:
            print("ZOOKEEPER:", realtime_output.strip(), flush=True)


def start_Kafka():
    print("KAFKA:", "Start process for Kafka")
    time_sleep = 20  # need at least 20s to let zookeeper started properly before start_up start
    for i in range(time_sleep):
        print("KAFKA:", "starting in {}sec ...".format(time_sleep-i+1))
        time.sleep(1)
    p_kafka = subprocess.Popen(KAFKA_HOME + '/start_up/windows/kafka-server-start.bat ./config/server.properties'
                               , shell=True
                               , stdout=subprocess.PIPE
                               , stderr=subprocess.STDOUT
                               , cwd=KAFKA_HOME
                               , encoding='utf-8'
                               , errors='replace'
                               )

    while True:
        realtime_output = p_kafka.stdout.readline()
        if realtime_output == '' and p_kafka.poll() is not None:
            break
        if realtime_output:
            print("KAFKA:", realtime_output.strip(), flush=True)


if __name__ == '__main__':
    with ProcessPoolExecutor(max_workers=2) as Executor:
        for ins in [start_ZooKeeper, start_Kafka]:
            Executor.submit(ins)
