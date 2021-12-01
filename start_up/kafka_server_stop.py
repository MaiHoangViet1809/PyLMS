import subprocess
import os, stat, shutil,  glob
import time

KAFKA_HOME = "D:/Kafka/kafka_2.13-2.8.1"

p_kafka = subprocess.Popen(KAFKA_HOME+'/start_up/windows/kafka-server-stop.bat', shell=True, cwd=KAFKA_HOME)
p_zookeeper = subprocess.Popen(KAFKA_HOME+'/start_up/windows/zookeeper-server-stop.bat', shell=True, cwd=KAFKA_HOME)


def del_path(p):
    # noinspection PyBroadException
    try:
        if os.path.isdir(p):
            shutil.rmtree(p)
        else:
            os.remove(p)
    except:
        os.chmod(p, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
        del_path(p)


files = glob.glob('D:\kafka\logs\kafka\*', recursive=True)
for f in files:
    is_done = False
    while not is_done:
        # noinspection PyBroadException
        try:
            del_path(f)
            is_done = True
        except:
            time.sleep(1)

files = glob.glob('D:\kafka\logs\zookeeper\*', recursive=True)
for f in files:
    is_done = False
    while not is_done:
        # noinspection PyBroadException
        try:
            del_path(f)
            is_done = True
        except:
            time.sleep(1)