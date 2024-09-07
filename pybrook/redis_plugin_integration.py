from typing import Literal

from pydantic import BaseModel


class DependencyField(BaseModel):
    type: Literal["Regular"] = "Regular"
    src: str
    dst: str


class HistoricalDependencyField(BaseModel):
    type: Literal["Historical"] = "Historical"
    src: str
    dst: str
    history_len: int


class Dependency(BaseModel):
    stream_key: str
    fields: list[DependencyField | HistoricalDependencyField] = []


class DependencyResolver(BaseModel):
    inputs: list[Dependency] = []
    output_stream_key: str


class InputTagger(BaseModel):
    stream_key: str
    obj_id_field: str


class BrookConfig(BaseModel):
    dependency_resolvers: dict[str, DependencyResolver] = {}
    input_taggers: dict[str, InputTagger] = {}
