from pydantic import create_model
from vo_models.uws.models import JobSummary, MultiValuedParameter, Parameter, Parameters


def create_parameter(name: str, value, by_reference: bool = False):
    if isinstance(value, (list, tuple)):
        param = (MultiValuedParameter, [Parameter(value=item, id=name, by_reference=by_reference) for item in value])
    else:
        param = (Parameter, Parameter(value=value, id=name, by_reference=by_reference))

    return param


def create_parameters(**kwargs) -> Parameters | None:
    if not kwargs:
        return

    fields = {}

    for name, value in kwargs.items():
        by_reference = False
        if name == "position":
            by_reference = True

        if isinstance(value, dict):
            for sub_name, sub_value in value.items():
                param_name = f"{name}.{sub_name}"
                fields[param_name] = create_parameter(name=param_name, value=sub_value, by_reference=by_reference)
        else:
            fields[name] = create_parameter(name=name, value=value, by_reference=by_reference)

    return create_model("CutoutsDynamicParameters", __base__=Parameters, **fields)()


def create_job_summary(job_id: str, parameters: dict = {}, **kwargs) -> JobSummary:
    job_parameters = create_parameters(**parameters)
    JobParameters = type(job_parameters)

    return JobSummary[JobParameters](job_id=job_id, parameters=job_parameters, **kwargs)
