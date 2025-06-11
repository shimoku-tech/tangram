import vertexai
from vertexai.generative_models import GenerativeModel
from shimoku_tangram.utils.environment import retrieve_google_env


def ask_model(
    prompt: str,
    model_id: str = "gemini-2.0-flash",
    project_id: str | None = None,
    region: str | None = None,
) -> str | None:
    project_id, region = retrieve_google_env(project_id, region)
    vertexai.init(project=project_id, location=region)
    model = GenerativeModel(model_id)
    return model.generate_content(prompt).text
