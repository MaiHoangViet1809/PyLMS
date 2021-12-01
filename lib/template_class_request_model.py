from pydantic import BaseModel, Field
from typing import List, Dict, Optional
from uuid import uuid4
from datetime import date, datetime


def uuid_generator() -> str:
    return uuid4().hex


class history_model(BaseModel):
    process_order: int
    process_name: str
    request_start_dttm: str
    request_finish_dttm: str
    request_body: Optional[Dict] = None
    response_body: Optional[Dict]  = None


class worker_model(BaseModel):
    msg_no : str  # = Field(default_factory=uuid_generator)
    topic_name : Optional[str] = ""
    next_stage : str
    contract_info : Dict
    status_history : List[history_model]
    track_change : Optional[List[Dict]] = None


if __name__ == '__main__':
    new_obj = history_model(process_order='2', process_name="test", request_dttm="test time")
    print(new_obj.json())

