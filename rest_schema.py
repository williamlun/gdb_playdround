import pydantic

# pylint: disable=invalid-name


class Site(pydantic.BaseModel):
    id: str
    name: str
    displayname: str
    tenantId: str
