class CutoutJobNotFoundError(Exception):
    def __init__(self, job_id: str):
        self.job_id = job_id
        super().__init__(f"Cutout job {job_id} not found")


class CutoutLimitExceededError(Exception):
    def __init__(self, limit: int, used: int, requested: int, retry_after: int):
        self.limit = limit
        self.used = used
        self.requested = requested
        self.retry_after = retry_after
        super().__init__(
            f"Cutout limit exceeded: {used} used + at least {requested} requested > {limit} limit "
            f"(retry after {retry_after}s)"
        )


class PrincipalResolutionError(Exception):
    def __init__(self, provider: str, error: str):
        self.provider = provider
        self.error = error
        super().__init__(f"Auth provider {provider} failed to resolve principal: {error}")
