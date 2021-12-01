# import lib.kafka_consumer as kc
#
# print("start read message")
# n = 0
#
# with kc.KafkaConsumerExtend() as consumer:
#     for message in consumer.polling(topic_name='LOAN_1'):
#         n += 1
#         print('Order id: {} - message: {}'.format(n, message))
#
from lib.template_class_request_model import history_model


def update_history(dict_body: dict):
    list_history = [history_model(**d) for d in dict_body['status_history']]
    last_step = {list_history.index(g): g.process_order for g in list_history}
    last_step = max(last_step, key=last_step.get)
    print(last_step)

info = {"msg_no": "awdawdawd",
        "topic_name": "",
        "next_stage": "create_loan",
        "contract_info": {"customer": {"cust_id": 11111,
                                       "customer_name": "nguyen van a",
                                       "dob": "1995/05/22"},
                          "lead_id": "123456789",
                          "loan_id": "",
                          "tenure": 9,
                          "interest_rate": 0.3,
                          "requested_amount": 15000000},
        "status_history": [{"process_order": 1,
                            "process_name": "NEW",
                            "request_start_dttm": "aaaaa",
                            "request_finish_dttm": "aaaaa",
                            "request_body": {},
                            "response_body": {}}
                           ]
        }
update_history(info)
