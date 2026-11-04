from pydantic import BaseModel, Field, ConfigDict
from typing import Literal
class TaskInput(BaseModel):
    model_config=ConfigDict(extra='forbid')
    name: str=Field(default='协同视频分析',max_length=120,min_length=1)
    source: str='demo'
    algorithm: str='resource_aware'
    segment_seconds: float=Field(default=4,ge=1,le=120)
    sample_fps: float=Field(default=2,ge=0.2,le=15)
    confidence: float=Field(default=0.35,ge=0.05,le=0.95)
    priority: int=Field(default=0,ge=0,le=10)
    deadline_seconds: int=Field(default=600,ge=10,le=86400)
    attempt_timeout: int=Field(default=90,ge=5,le=3600)
    max_retries: int=Field(default=2,ge=0,le=5)
    min_memory_mb: int=Field(default=256,ge=64,le=65536)
    node_ids: list[str]=Field(default_factory=list,max_length=64)
    required_tags: list[str]=Field(default_factory=list,max_length=20)
    architecture: str|None=None
    location: str|None=Field(default=None,max_length=100)
    workflow_id: str='video-analysis'
class NodeInput(BaseModel):
    id: str=Field(pattern=r'^[a-zA-Z0-9_-]{1,64}$')
    endpoint: str
    cluster_id: str='external'
    kind: Literal['server_process','vehicle','simulated']='server_process'
class StrategyInput(BaseModel):
    name: str=Field(min_length=1,max_length=120)
    kind: Literal['service','periodic']='periodic'
    interval_seconds: int=Field(default=300,ge=15,le=86400)
    enabled: bool=True
    params: TaskInput=Field(default_factory=TaskInput)

class CandidateInput(BaseModel):
    model_config=ConfigDict(extra='forbid')
    params: TaskInput=Field(default_factory=TaskInput)
    duration_seconds: float=Field(default=4,gt=0,le=600)
    input_bytes: int=Field(default=0,ge=0,le=536870912)
