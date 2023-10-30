from pydantic import Field

from models.model import Model


class Url(Model):
    url: str


class Film(Model):
    id: str = Field(..., alias="uuid")
    title: str
    imdb_rating: float | None = None
    genre: list[dict] | None = Field(None, alias="genres")
    description: str | None = None
    directors: list[dict] | None = Field(None, alias="director")
    actors_names: list[str] | None = None
    writers_names: list[str] | None = None
    actors: list[dict] | None = None
    writers: list[dict] | None = None
