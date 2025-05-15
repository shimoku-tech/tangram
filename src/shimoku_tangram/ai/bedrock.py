import json
from boto3 import client

def ask_model(
    prompt: str,
    model_id: str = 'eu.amazon.nova-micro-v1:0'
) -> str | None:
    br = client('bedrock-runtime')
    request = {"messages": [
        {"role": "user", "content": [{"text": prompt}]}
    ]}
    response = br.invoke_model(
        modelId=model_id,
        body=json.dumps(request)
    )
    model_response = json.loads(response["body"].read())
    response_text = model_response["output"]["message"]["content"][0]["text"]
    return response_text
