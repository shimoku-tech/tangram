from shimoku_tangram.reporting.logging import init_logger
from unittest import TestCase
import logging


class TestLogging(TestCase):
    def test_init_logger_no_level(self):
        logger = init_logger("test_logger")
        self.assertEqual(logger.level, logging.ERROR)

    def test_init_logger_with_level(self):
        logger = init_logger("test_logger", logging.DEBUG)
        self.assertEqual(logger.level, logging.DEBUG)

    def test_init_logger_formatter(self):
        logger = init_logger("test_logger")
        self.assertEqual(
            logger.handlers[0].formatter._fmt,
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )
