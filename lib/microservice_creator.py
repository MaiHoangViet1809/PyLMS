import uvicorn
import argparse
import os
from lib.template_class_api_ms import MS_API, unregister_api

parser = argparse.ArgumentParser(description='Create microservice worker for process msg')
parser.add_argument('-ms_host', type=str, help='IP of this instance (example: 127.0.0.1)')
parser.add_argument('-ms_port', type=int, help='port of this instance (example: 8000)')
parser.add_argument('-ms_workers', type=int, help='number or core use for this instance (default 1).', default=1)
parser.add_argument('-ms_log_level', type=str, help='critical/warning/info/traceback (default info).', default="info")
args = parser.parse_args()

file_name = os.path.splitext(os.path.basename(__file__))[0]
host = args.ms_host
port = args.ms_port
workers = args.ms_workers
log_level = args.ms_log_level

if __name__ == '__main__':
    assert host is not None, "missing argument host"
    assert port is not None, "missing argument port"

    print("worker {host}:{port} is starting".format(host=host, port=port))
    try:
        app = MS_API(host=host, port=port)
        app.register_api()

        if workers > 1:
            uvicorn.run("{}:app".format(file_name),
                        host=host,
                        port=port,
                        debug=False,
                        log_level=log_level,
                        workers=workers
                        , limit_concurrency=1000
                        , timeout_keep_alive=120
                        )
        else:
            uvicorn.run(app, host=host, port=port, debug=False, log_level=log_level
                        ,limit_concurrency=1000
                        ,timeout_keep_alive=120
                        )
    finally:
        print("worker {host}:{port} is stopped".format(host=host, port=port))
        try:
            unregister_api(service_name='worker', host=host, port=port)
        finally:
            print("worker {host}:{port} is unregistered!".format(host=host, port=port))


