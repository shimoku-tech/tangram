from google.cloud import secretmanager
from shimoku_tangram.utils.environment import retrieve_google_env
import json


def get_secret(
    secret_name: str,
    is_dict: bool = False,
    project_id: str | None = None,
    region: str | None = None,
) -> str | dict:
    project_id, region = retrieve_google_env(project_id, region)
    secret_client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = secret_client.access_secret_version(request={"name": secret_path})
    response = response.payload.data.decode("UTF-8")
    if is_dict:
        return json.loads(response)
    return response


def put_secret(
    secret_name: str,
    secret_value: str | dict,
    project_id: str | None = None,
    region: str | None = None,
) -> bool:
    project_id, region = retrieve_google_env(project_id, region)
    secret_client = secretmanager.SecretManagerServiceClient()
    parent = f"projects/{project_id}"

    if not isinstance(secret_value, str):
        secret_value = json.dumps(secret_value)

    try:
        # Create the secret
        secret = secret_client.create_secret(
            request={
                "parent": parent,
                "secret_id": secret_name,
                "secret": {"replication": {"automatic": {}}},
            }
        )

        # Add the secret version with the actual value
        secret_client.add_secret_version(
            request={
                "parent": secret.name,
                "payload": {"data": secret_value.encode("UTF-8")},
            }
        )

        return True

    except Exception as e:
        # If secret already exists, try to add a new version
        try:
            secret_path = f"projects/{project_id}/secrets/{secret_name}"
            secret_client.add_secret_version(
                request={
                    "parent": secret_path,
                    "payload": {"data": secret_value.encode("UTF-8")},
                }
            )
            return True
        except Exception:
            print(f"Error creating/updating secret: {e}")
            return False


def delete_secret(
    secret_name: str, project_id: str | None = None, region: str | None = None
) -> bool:
    project_id, region = retrieve_google_env(project_id, region)
    secret_client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{project_id}/secrets/{secret_name}"
    try:
        secret_client.delete_secret(request={"name": secret_path})
        return True
    except Exception as e:
        print(f"Error deleting secret: {e}")
        return False
