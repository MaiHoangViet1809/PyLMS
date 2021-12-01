import json
import random
import string
import uuid
import time
import httpx
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
from lib.kafka_producer import KafkaProducerExtend

info_big = [{"msg_no": uuid.uuid4().hex,
        "topic_name": "",
        "next_stage": "create_loan",
        "contract_info": {"customer": {"cust_id": random.randint(100000, 999999),
                                       "customer_name": "nguyen van {}"
                                       .format(''.join(random.choices(string.ascii_uppercase + string.digits,
                                                                      k=5))),
                                       "dob": "1995/05/22"},
                          "lead_id": "123456789",
                          "loan_id": "",
                          "tenure": 9,
                          "interest_rate": 0.3,
                          "requested_amount": 15000000},
        "status_history": [{"process_order": 1,
                            "process_name": "NEW",
                            "request_dttm": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
                            "request_body": {},
                            "response_body": {}}
                           ]
        },
        {"msg_no": uuid.uuid4().hex,
        "topic_name": "",
        "next_stage": "create_loan",
        "contract_info": {"customer": {"cust_id": random.randint(100000, 999999),
                                       "customer_name": "nguyen van {}"
                                       .format(''.join(random.choices(string.ascii_uppercase + string.digits,
                                                                      k=5))),
                                       "dob": "1995/05/22"},
                          "lead_id": "123456789",
                          "loan_id": "",
                          "tenure": 9,
                          "interest_rate": 0.3,
                          "requested_amount": 15000000},
        "status_history": [{"process_order": 1,
                            "process_name": "NEW",
                            "request_dttm": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
                            "request_body": {},
                            "response_body": {}}
                           ]
        },
        {"msg_no": uuid.uuid4().hex,
         "topic_name": "",
         "next_stage": "create_loan",
         "contract_info": {"customer": {"cust_id": random.randint(100000, 999999),
                                        "customer_name": "nguyen van {}"
                                            .format(''.join(random.choices(string.ascii_uppercase + string.digits,
                                                                           k=5))),
                                        "dob": "1995/05/22"},
                           "lead_id": "123456789",
                           "loan_id": "",
                           "tenure": 9,
                           "interest_rate": 0.3,
                           "requested_amount": 15000000},
         "status_history": [{"process_order": 1,
                             "process_name": "NEW",
                             "request_dttm": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
                             "request_body": {},
                             "response_body": {}}
                            ]
         },
        {"msg_no": uuid.uuid4().hex,
         "topic_name": "",
         "next_stage": "create_loan",
         "contract_info": {"customer": {"cust_id": random.randint(100000, 999999),
                                        "customer_name": "nguyen van {}"
                                            .format(''.join(random.choices(string.ascii_uppercase + string.digits,
                                                                           k=5))),
                                        "dob": "1995/05/22"},
                           "lead_id": "123456789",
                           "loan_id": "",
                           "tenure": 9,
                           "interest_rate": 0.3,
                           "requested_amount": 15000000},
         "status_history": [{"process_order": 1,
                             "process_name": "NEW",
                             "request_dttm": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
                             "request_body": {},
                             "response_body": {}}
                            ]
         },
        {"msg_no": uuid.uuid4().hex,
         "topic_name": "",
         "next_stage": "create_loan",
         "contract_info": {"customer": {"cust_id": random.randint(100000, 999999),
                                        "customer_name": "nguyen van {}"
                                            .format(''.join(random.choices(string.ascii_uppercase + string.digits,
                                                                           k=5))),
                                        "dob": "1995/05/22"},
                           "lead_id": "123456789",
                           "loan_id": "",
                           "tenure": 9,
                           "interest_rate": 0.3,
                           "requested_amount": 15000000},
         "status_history": [{"process_order": 1,
                             "process_name": "NEW",
                             "request_dttm": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
                             "request_body": {},
                             "response_body": {}}
                            ]
         },
        {"msg_no": uuid.uuid4().hex,
         "topic_name": "",
         "next_stage": "create_loan",
         "contract_info": {"customer": {"cust_id": random.randint(100000, 999999),
                                        "customer_name": "nguyen van {}"
                                            .format(''.join(random.choices(string.ascii_uppercase + string.digits,
                                                                           k=5))),
                                        "dob": "1995/05/22"},
                           "lead_id": "123456789",
                           "loan_id": "",
                           "tenure": 9,
                           "interest_rate": 0.3,
                           "requested_amount": 15000000},
         "status_history": [{"process_order": 1,
                             "process_name": "NEW",
                             "request_dttm": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
                             "request_body": {},
                             "response_body": {}}
                            ]
         },
        {"msg_no": uuid.uuid4().hex,
         "topic_name": "",
         "next_stage": "create_loan",
         "contract_info": {"customer": {"cust_id": random.randint(100000, 999999),
                                        "customer_name": "nguyen van {}"
                                            .format(''.join(random.choices(string.ascii_uppercase + string.digits,
                                                                           k=5))),
                                        "dob": "1995/05/22"},
                           "lead_id": "123456789",
                           "loan_id": "",
                           "tenure": 9,
                           "interest_rate": 0.3,
                           "requested_amount": 15000000},
         "status_history": [{"process_order": 1,
                             "process_name": "NEW",
                             "request_dttm": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
                             "request_body": {},
                             "response_body": {}}
                            ]
         },
        {"msg_no": uuid.uuid4().hex,
         "topic_name": "",
         "next_stage": "create_loan",
         "contract_info": {"customer": {"cust_id": random.randint(100000, 999999),
                                        "customer_name": "nguyen van {}"
                                            .format(''.join(random.choices(string.ascii_uppercase + string.digits,
                                                                           k=5))),
                                        "dob": "1995/05/22"},
                           "lead_id": "123456789",
                           "loan_id": "",
                           "tenure": 9,
                           "interest_rate": 0.3,
                           "requested_amount": 15000000},
         "status_history": [{"process_order": 1,
                             "process_name": "NEW",
                             "request_dttm": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
                             "request_body": {},
                             "response_body": {}}
                            ]
         },
        {"msg_no": uuid.uuid4().hex,
         "topic_name": "",
         "next_stage": "create_loan",
         "contract_info": {"customer": {"cust_id": random.randint(100000, 999999),
                                        "customer_name": "nguyen van {}"
                                            .format(''.join(random.choices(string.ascii_uppercase + string.digits,
                                                                           k=5))),
                                        "dob": "1995/05/22"},
                           "lead_id": "123456789",
                           "loan_id": "",
                           "tenure": 9,
                           "interest_rate": 0.3,
                           "requested_amount": 15000000},
         "status_history": [{"process_order": 1,
                             "process_name": "NEW",
                             "request_dttm": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
                             "request_body": {},
                             "response_body": {}}
                            ]
         },
        {"msg_no": uuid.uuid4().hex,
         "topic_name": "",
         "next_stage": "create_loan",
         "contract_info": {"customer": {"cust_id": random.randint(100000, 999999),
                                        "customer_name": "nguyen van {}"
                                            .format(''.join(random.choices(string.ascii_uppercase + string.digits,
                                                                           k=5))),
                                        "dob": "1995/05/22"},
                           "lead_id": "123456789",
                           "loan_id": "",
                           "tenure": 9,
                           "interest_rate": 0.3,
                           "requested_amount": 15000000},
         "status_history": [{"process_order": 1,
                             "process_name": "NEW",
                             "request_dttm": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
                             "request_body": {},
                             "response_body": {}}
                            ]
         },
        {"msg_no": uuid.uuid4().hex,
         "topic_name": "",
         "next_stage": "create_loan",
         "contract_info": {"customer": {"cust_id": random.randint(100000, 999999),
                                        "customer_name": "nguyen van {}"
                                            .format(''.join(random.choices(string.ascii_uppercase + string.digits,
                                                                           k=5))),
                                        "dob": "1995/05/22"},
                           "lead_id": "123456789",
                           "loan_id": "",
                           "tenure": 9,
                           "interest_rate": 0.3,
                           "requested_amount": 15000000},
         "status_history": [{"process_order": 1,
                             "process_name": "NEW",
                             "request_dttm": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
                             "request_body": {},
                             "response_body": {}}
                            ]
         },
        ]


def UAT_create_loan_2(index, k: KafkaProducerExtend):
    info = {"msg_no": uuid.uuid4().hex,
            "topic_name": "",
            "next_stage": "create_loan",
            "contract_info": {"customer": {"cust_id": random.randint(100000, 999999),
                                           "customer_name": "nguyen van {}"
                                           .format(''.join(random.choices(string.ascii_uppercase + string.digits,
                                                                          k=5))),
                                           "dob": "1995/05/22"},
                              "lead_id": "123456789",
                              "loan_id": "",
                              "tenure": 9,
                              "interest_rate": 0.3,
                              "requested_amount": 15000000},
            "status_history": [{"process_order": 1,
                                "process_name": "NEW",
                                "request_dttm": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
                                "request_body": {},
                                "response_body": {}}
                               ]
            }
    # info = info_big
    k.send_msg(topic_name='LOAN_1', value=json.dumps(info))
    return index


# noinspection PyBroadException
def UAT_create_loan(index) -> str:
    info = {"msg_no": uuid.uuid4().hex,
            "topic_name": "",
            "next_stage": "create_loan",
            "contract_info": {"customer": {"cust_id": random.randint(100000, 999999),
                                           "customer_name": "nguyen van {}"
                                           .format(''.join(random.choices(string.ascii_uppercase + string.digits,
                                                                          k=5))),
                                           "dob": "1995/05/22"},
                              "lead_id": "123456789",
                              "loan_id": "",
                              "tenure": 9,
                              "interest_rate": 0.3,
                              "requested_amount": 15000000},
            "status_history": [{"process_order": 1,
                                "process_name": "NEW",
                                "request_dttm": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
                                "request_body": {},
                                "response_body": {}}
                               ]
            }

    while True:
        try:
            # result = ws.send(json.dumps(info))
            g = httpx.post(url='http://127.0.0.1:8000/LOAN_1', json=info)
            if g:
                dict_response = g.json()
                if 'service_result' in dict_response:
                    if dict_response['service_result'] == 'OK':
                        return str(index)
                print(g)
            time.sleep(1)
        except Exception as E:
            print(E)
            time.sleep(5)


def test_1():
    with ThreadPoolExecutor(max_workers=10) as executor:
        start_time = time.time()
        start_dttm = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        total_msg = 20000
        try:
            future_submit = [executor.submit(UAT_create_loan, i) for i in range(0, total_msg)]
            for future in as_completed(future_submit):
                print("finish: " + str(future.result()))
            end_time = time.time()
            end_dttm = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        finally:
            print(start_dttm, end_dttm, round(end_time-start_time, 2), round(total_msg /(end_time-start_time), 2))


def test_2(num):
    current_thread = "thread[{}]".format(num)
    with ThreadPoolExecutor(max_workers=12) as executor, KafkaProducerExtend() as K:
        start_time = time.time()
        start_dttm = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        total_msg = num
        future_submit = [executor.submit(UAT_create_loan_2, i, K) for i in range(0, total_msg)]
        i = 0
        for future in as_completed(future_submit):
            i += 1
            # end_time = time.time()
            # print('thread:{} msg_no:{} running:{}/{} speed:{}'.format(current_thread,
            #                                                           future.result(),
            #                                                           i,
            #                                                           total_msg,
            #                                                           round(i/(end_time-start_time), 2)))
        end_time = time.time()
        end_dttm = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        print("{} total msg:{} start_time:{} end_time:{} eslaped sec:{} msg/s:{}"
              .format(current_thread,
                      total_msg,
                      start_dttm,
                      end_dttm,
                      round(end_time-start_time, 2),
                      round(total_msg /(end_time-start_time), 2)))


if __name__ == '__main__':
    with ProcessPoolExecutor(max_workers=6) as PE:
        list_F = [PE.submit(test_2, i) for i in [25000, 25001, 25002, 25003, 25004, 25005]]
        for f in as_completed(list_F):
            print("done")


