from os import getenv
from functools import lru_cache


@lru_cache(maxsize=128)
def retrieve_google_env(project_id: str | None, region: str | None):
    project_id = project_id or getenv("GOOGLE_PROJECT_ID")
    if project_id is None:
        raise ValueError(
            "Parameter 'project_id' must be provided or set as the "
            "environment variable 'GOOGLE_PROJECT_ID'"
        )
    region = region or getenv("GOOGLE_REGION", "europe-west1")
    return project_id, region
