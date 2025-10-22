import json

from pydantic import BaseModel

from fornax_cutouts import models


class PydanticSerializer(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, BaseModel):
            return obj.model_dump()
        else:
            return json.JSONEncoder.default(self, obj)


def pydantic_decoder(obj):
    if '__type__' in obj:
        if obj['__type__'] in dir(models):
            cls = getattr(models, obj['__type__'])
            return cls.parse_obj(obj)
    return obj

def pydantic_dumps(obj):
    return json.dumps(obj, cls=PydanticSerializer)


def pydantic_loads(obj):
    return json.loads(obj, object_hook=pydantic_decoder)
