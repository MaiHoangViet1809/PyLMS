import argparse

parser = argparse.ArgumentParser(description='Create microservice worker for process msg')
parser.add_argument('-ms_host', type=str, help='IP of this instance (example: 127.0.0.1)')
parser.add_argument('-ms_port', type=int, help='port of this instance (example: 8000)')
parser.add_argument('-workers', type=int, help='number or core use for this instance (default 1).', default=1)
args = parser.parse_args()

print(args)
print(args.ms_host, args.ms_port)