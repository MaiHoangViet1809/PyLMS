import uuid
import time
import json
from fastapi import HTTPException
from lib.template_class_process import base, topic_index
from lib.template_class_request_model import history_model


async def update_history(dict_body: dict, stage: str, start_time: str,
                         request_body: dict = None, response_body: dict = None):
    # validate history data using pydantic model
    list_history = [history_model(**d) for d in dict_body['status_history']]

    # find lastest step
    temp = {list_history.index(g): g.process_order for g in list_history}
    temp = max(temp, key=temp.get)
    last_step: history_model = list_history[temp]

    # create current stage
    current_stage_order = last_step.process_order + 1
    current_stage = history_model(process_order=current_stage_order,
                                  process_name=stage,
                                  request_start_dttm=start_time,
                                  request_finish_dttm=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
                                  request_body=request_body,
                                  response_body=response_body
                                  )
    return dict_body['status_history'].appends(current_stage)


async def create_loan(req_body: str):
    # mandatory step
    start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

    # main process
    topic = topic_index()
    topic_name = topic.available_topic('loan')
    contract = base(json_data=req_body, topic=topic_name)
    contract.unlock()

    contract_detail = contract.contract_info

    # generate contract_no
    contract_detail['loan_id'] = uuid.uuid4().hex

    # check requested field
    res = []
    list_chk = ['tenure', 'interest_rate', 'requested_amount']
    for chk in list_chk:
        if chk not in contract_detail:
            res.append(chk)

    if len(res) > 0:
        raise HTTPException(status_code=401, detail="{} is missing in request body".format(', '.join(res)))

    contract_detail['approved_amount'] = contract_detail['requested_amount'] * 0.8

    # commit
    contract.next_stage = 'disb_loan'
    contract.contract_info = contract_detail

    # mandatory:
    contract.status_history = await update_history(dict_body=json.loads(req_body),
                                                   stage='create_loan',
                                                   start_time=start_time)

    contract.is_dirty = True
    msg_response = await contract.commit()
    return msg_response


async def disb_loan(req_body: str):
    contract = base(json_data=req_body)
    contract.unlock()

    contract_detail = contract.contract_info

    # calculate payment schedule
    res = []
    list_chk = ['tenure', 'interest_rate', 'approved_amount']
    for chk in list_chk:
        if chk not in contract_detail:
            res.append(chk)
    if len(res) > 0:
        raise HTTPException(status_code=401, detail="{} is missing in request body".format(', '.join(res)))

    tenure, interest_rate, approved_amount = [contract_detail[index] for index in list_chk]
    payment_schedule = [{"instnum": i,
                         "interest_amount": approved_amount * interest_rate / 12,
                         "principle_amount": approved_amount / tenure,
                         "ENR": approved_amount * interest_rate / 12 + approved_amount / tenure}
                        for i in range(1, tenure)]
    contract_detail['payment_schedule'] = payment_schedule

    # commit
    contract.next_stage = None
    contract.contract_info = contract_detail
    contract.is_dirty = True
    response_msg = await contract.commit()

    return response_msg
