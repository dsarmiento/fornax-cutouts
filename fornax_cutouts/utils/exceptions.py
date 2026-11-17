class CutoutJobNotFoundError(Exception):
    def __init__(self, job_id: str):
        self.job_id = job_id
        super().__init__(f"Cutout job {job_id} not found")


class CutoutLimitExceededError(Exception):
    def __init__(
        self,
        limit: int,
        used: int,
        requested: int,
        retry_after: int,
        limit_hit_info: str = "",
    ):
        self.limit = limit
        self.used = used
        self.requested = requested
        self.retry_after = retry_after
        self.limit_hit_info = limit_hit_info

        message = (
            f"Cutout limit exceeded: {used} used + at least {requested} requested > {limit} limit "
            f"(retry after {retry_after}s)"
        )
        if limit_hit_info:
            message = f"{message}\n{limit_hit_info}"
        super().__init__(message)


class PrincipalResolutionError(Exception):
    def __init__(self, provider: str, error: str):
        self.provider = provider
        self.error = error
        super().__init__(f"Auth provider {provider} failed to resolve principal: {error}")


class NoTasksRemainingInJobError(Exception):
    def __init__(self, job_id: str):
        self.job_id = job_id
        super().__init__(f"No cutout tasks remaining in {job_id}")
