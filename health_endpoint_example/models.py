#! usr/bin/env python 3.7
from typing import Optional
from collections import deque

from pydantic import BaseModel


class RabbitMQHealth(BaseModel):
    is_connected: Optional[bool]
    host: Optional[str]
    port: Optional[int]
    user: Optional[str]


class HeathReport(BaseModel):
    status: str = "ok"
    rabbit_mq: Optional[RabbitMQHealth]
    recent_response_times: Optional[deque]
    recent_response_times_average: Optional[float]
    last_response_detail: Optional[str]


