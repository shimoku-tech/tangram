from shimoku_tangram.reporting import sentry
from unittest import TestCase
from unittest.mock import patch


class TestSentrySetup(TestCase):
    @patch("shimoku_tangram.reporting.sentry.AwsLambdaIntegration")
    @patch("shimoku_tangram.reporting.sentry.init")
    @patch("shimoku_tangram.reporting.sentry.getenv")
    def test_init_sentry_without_provided_dsn(
        self, mock_getenv, mock_sentry_init, mock_aws_lambda_integration
    ):
        mock_aws_lambda_integration.return_value = "mocked_aws_lambda_integration"
        mock_getenv.return_value = "mocked_dsn_from_env"

        sentry.init_sentry()

        mock_sentry_init.assert_called_once_with(
            dsn="mocked_dsn_from_env",
            races_sample_rate=1.0,
            integrations=["mocked_aws_lambda_integration"],
        )

    @patch("shimoku_tangram.reporting.sentry.AwsLambdaIntegration")
    @patch("shimoku_tangram.reporting.sentry.init")
    @patch("shimoku_tangram.reporting.sentry.getenv")
    def test_init_sentry_with_provided_dsn(
        self, mock_getenv, mock_sentry_init, mock_aws_lambda_integration
    ):
        mock_aws_lambda_integration.return_value = "mocked_aws_lambda_integration"
        mock_getenv.return_value = "mocked_dsn_from_env"

        sentry.init_sentry(sentry_dsn="provided_dsn")

        mock_getenv.assert_not_called()

        mock_sentry_init.assert_called_once_with(
            dsn="provided_dsn",
            races_sample_rate=1.0,
            integrations=["mocked_aws_lambda_integration"],
        )
