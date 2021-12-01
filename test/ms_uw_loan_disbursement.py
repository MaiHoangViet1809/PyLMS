from fastapi import HTTPException
import uvicorn
import os
from lib.template_class_process import base
from lib.template_class_api_ms import MS_API


async def disb_loan(req_body: str):
    contract = base(json_data=req_body)
    contract.unlock()

    # calculate payment schedule
    res = []
    list_chk = ['tenure', 'interest_rate', 'approved_amount']
    for chk in list_chk:
        if chk not in contract.__dict__:
            res.append(chk)
    if len(res) > 0:
        raise HTTPException(status_code=401, detail="{} is missing in request body".format(', '.join(res)))

    tenure, interest_rate, approved_amount = [contract.__dict__[index] for index in list_chk]
    payment_schedule = [(i,
                         approved_amount * interest_rate / 12,
                         approved_amount / tenure,
                         approved_amount * interest_rate / 12 + approved_amount / tenure)
                        for i in range(1, tenure)]
    contract.payment_schedule = payment_schedule
    contract.next_action = None

    # create base class
    contract.is_dirty = True
    response_msg = await contract.commit()

    return response_msg


service_name = os.path.splitext(os.path.basename(__file__))[0]
host = "127.0.0.1"
port = 8003

app = MS_API(service_name=service_name, callback=disb_loan, host=host, port=port)
app.register_api()

if __name__ == '__main__':
    # uvicorn.run("{service_name}:app".format(service_name=service_name), host=host, port=port, debug=False
    #             # , workers=2
    #             )
    uvicorn.run(app, host=host, port=port, debug=False)