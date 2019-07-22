#! usr/bin/env python 3.7
from typing import Optional, List

from pydantic import BaseModel


class RabbitMQHealth(BaseModel):
    is_connected: Optional[bool]
    connection_properties: Optional[str]
    success_message_count: Optional[int]
    dropped_message_count: Optional[int]
    connection_failure_count: Optional[int]


class HealthReport(BaseModel):
    status: str = "starting"
    service_start_date: Optional[str]
    status_date: Optional[str]
    rabbit_mq: Optional[RabbitMQHealth]
    recent_response_times: Optional[List]
    recent_response_times_average: Optional[float]
    last_response_detail: Optional[str]


class Message(BaseModel):
    sleep_time: int
    conversation_id: str
