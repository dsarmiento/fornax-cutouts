from pydantic import create_model
from vo_models.uws.models import JobSummary, MultiValuedParameter, Parameter, Parameters


def create_parameter(name: str, value):
    if isinstance(value, (list, tuple)):
        param = (MultiValuedParameter, [Parameter(value=item, id=name) for item in value])
    else:
        param = (Parameter, Parameter(value=value, id=name))

    return param

def create_parameters(**kwargs) -> Parameters | None:
    if not kwargs:
        return

    fields = {}

    for name, value in kwargs.items():
        if isinstance(value, dict):
            for sub_name, sub_value in value.items():
                param_name = f"{name}.{sub_name}"
                fields[param_name] = create_parameter(name=param_name, value=sub_value)
        else:
            fields[name] = create_parameter(name=name, value=value)

    return create_model("CutoutsDynamicParameters", __base__=Parameters, **fields)

def create_job_summary(
    job_id: str,
    run_id: str | None = None,
    parameters: dict = {},
    **kwargs
) -> JobSummary:
    JobParameters = create_parameters(**parameters)

    return JobSummary[JobParameters](
        job_id=job_id,
        run_id=run_id,
        parameters=JobParameters(),
        **kwargs
    )
