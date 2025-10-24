from pydantic import BaseModel


class FilenameRequest(BaseModel):
    position: list[str] | None = None
    survey: list[str] | None = None
    filter: list[str] | None = None

    class Config:
        extra = "allow"
