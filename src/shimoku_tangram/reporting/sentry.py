from sentry_sdk import init
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration
from os import getenv


def init_sentry(sentry_dsn: str | None = None) -> None:
    init(
        dsn=sentry_dsn or getenv("SENTRY_DSN"),
        races_sample_rate=1.0,
        integrations=[AwsLambdaIntegration()],
    )


# Example on how to configure sentry extra info:
"""
with sentry_sdk.configure_scope() as scope:
    scope.set_tag("holi", "test_tag")
    scope.set_extra("extra_context", {"Adeu": "test_value"})

## Example on how to log custom errors to sentry
sentry_sdk.capture_message('Test Error', level='error')
"""
