# some wrapper to add functionality to class/function
import uvicorn
from concurrent.futures import ProcessPoolExecutor, as_completed
import time


class ms_generator:
    def __init__(self, class_type: type, num_instance: int, executor: ProcessPoolExecutor, **kwarg):
        self.instances = {}
        self.processes = []
        self.num = num_instance
        self._class_type = class_type
        self.executor = executor
        self.kwarg = kwarg
        self._create_instance()

    def _create_instance(self):
        for i in range(0, self.num):
            print(i, '/', range(0, self.num))
            app = self._class_type(**self.kwarg)
            while True:
                result = app.register_api()
                print('result: ', result, app.service_name, app.host, app.port)
                if not result:
                    app.port += 1
                    time.sleep(1)
                    continue
                break
            print('got here ', 1)
            future = self.executor.submit(uvicorn.run, app=app, host=app.host, port=app.port, debug=False)
            uvicorn.Server()
            print('got here ', 2)
            self.processes.append(future)
            print('got here ', 3)
            self.instances[app.service_uuid] = app
            print('got here ', 4)
        print('done init')
        for f in as_completed(self.processes):
            print(f.done(), f.result())
        print('all done')

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        for app_uuid in self.instances:
            self.instances[app_uuid].__del__()




