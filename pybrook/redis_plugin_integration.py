from pydantic import BaseModel


class DependencyField(BaseModel):
    src: str
    dst: str

class Dependency(BaseModel):
    stream_key: str
    fields: list[DependencyField] = []


class DependencyResolver(BaseModel):
    inputs: list[Dependency] = []
    output_stream_key: str


class InputTagger(BaseModel):
    stream_key: str
    obj_id_field: str


class BrookConfig(BaseModel):
    dependency_resolvers: dict[str, DependencyResolver] = {}
    input_taggers: dict[str, InputTagger] = {}