import json
from typing import Type, TypeVar

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, ValidationError

from fornax_cutouts.models.metadata import FilenameRequest
from fornax_cutouts.sources import cutout_registry

T = TypeVar("T", bound=BaseModel)


def check_json_string(v):
    """Check if the input is a valid JSON string and parse it."""
    if isinstance(v, str):
        # Treat an empty string like an empty dictionary so that users can pass in just an empty string and
        # assume default params for a mission
        if v == "":
            return {}
        try:
            return json.loads(v)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON string: {e.msg}") from e
    return v


def form_parser(model_type: Type[T]):
    """Parse form data from a FastAPI request into a Pydantic model.

    This function returns a callable that can be used as a dependency in FastAPI endpoints to automatically parse
    and validate form data against the specified Pydantic model. It has special handling for parsing
    source-specific fields and the "missions" field as JSON strings.

    Args:
        model_type (Type[T]): The Pydantic model class to parse the form data into.

    Returns:
        Callable[[Request], T]: A function that takes a FastAPI request and returns an instance of the model.
    """

    async def parse_request(request: Request) -> T:
        source_names = cutout_registry.get_source_names()
        form = await request.form()

        form_data = {}
        for key in form.keys():
            values = form.getlist(key)

            if key in source_names or key == "missions":
                try:
                    form_data[key] = check_json_string(values[0])
                except ValueError as e:
                    raise RequestValidationError([{"loc": ("form", key), "msg": str(e), "type": "invalid_json"}]) from e
            else:
                form_data[key] = values if len(values) > 1 else values[0]

        try:
            return model_type.model_validate(form_data)
        except ValidationError as e:
            raise RequestValidationError(e.errors())

    return parse_request


def _filename_params(fname_request: FilenameRequest) -> dict:
    return fname_request.model_dump(exclude={"position"}, exclude_none=True)
