import time
import httpx
from concurrent.futures import ThreadPoolExecutor


# noinspection PyBroadException
def push_log(msg, topic: str = 'application_log'):
    try:
        with ThreadPoolExecutor(max_workers=1) as Executor:
            Executor.submit(httpx.post,
                            url='http://127.0.0.1:8000/{topic_name}'.format(topic_name=topic),
                            json=msg)

        return True
    except:
        return False


def logging(service_name, action, msg, is_print: bool = False):
    if is_print:
        print('[{event_time}]-[{service_name}]-[action={action}]: {msg}'
              .format(event_time=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
                      service_name=service_name,
                      action=action,
                      msg=msg))
    else:
        return push_log('[{event_time}]-[{service_name}]-[action={action}]: {msg}'
                        .format(event_time=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
                                service_name=service_name,
                                action=action,
                                msg=msg
                                )
                        )



