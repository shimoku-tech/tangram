from shimoku_tangram.reporting.logging import init_logger, end_logger, XRayFilter
from unittest import TestCase
import logging
from unittest.mock import patch, MagicMock

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
            "%(asctime)s - %(name)s - %(levelname)s - [ %(trace_id)s ] - %(message)s",
        )

    def test_init_logger_filter(self):
        logger = init_logger("test_logger")
        self.assertEqual(
            logger.handlers[0].filters[0].__class__.__name__,
            "XRayFilter",
        )
    
    def test_end_logger(self):
        logger = init_logger("test_logger")
        end_logger(logger)

        self.assertEqual(
            logger.handlers,
            [],
        )

class TestXRayFilter(TestCase):
    def setUp(self):
        self.filter = XRayFilter()
        self.log_record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="test_path",
            lineno=10,
            msg="Test message",
            args=(),
            exc_info=None
        )

    @patch('shimoku_tangram.reporting.logging.xray_recorder')  # Replace 'shimoku_tangram.reporting.logging' with the actual module name
    def test_filter_with_segment(self, mock_xray_recorder):
        mock_segment = MagicMock()
        mock_segment.trace_id = "test_trace_id"
        mock_xray_recorder.current_segment.return_value = mock_segment

        result = self.filter.filter(self.log_record)

        self.assertTrue(result)
        self.assertEqual(self.log_record.trace_id, "test_trace_id")

    @patch('shimoku_tangram.reporting.logging.xray_recorder')  # Replace 'shimoku_tangram.reporting.logging' with the actual module name
    def test_filter_without_segment(self, mock_xray_recorder):
        mock_xray_recorder.current_segment.return_value = None

        result = self.filter.filter(self.log_record)

        self.assertTrue(result)
        self.assertIsNone(self.log_record.trace_id)

    def test_filter_always_returns_true(self):
        result = self.filter.filter(self.log_record)
        self.assertTrue(result)

    def test_filter_adds_trace_id_attribute(self):
        with patch('shimoku_tangram.reporting.logging.xray_recorder.current_segment') as mock_current_segment:
            mock_segment = MagicMock()
            mock_segment.trace_id = "test_trace_id"
            mock_current_segment.return_value = mock_segment

            self.filter.filter(self.log_record)

        self.assertTrue(hasattr(self.log_record, 'trace_id'))

    def test_filter_sets_trace_id_to_none_when_no_segment(self):
        with patch('shimoku_tangram.reporting.logging.xray_recorder.current_segment', return_value=None):
            self.filter.filter(self.log_record)

        self.assertIsNone(self.log_record.trace_id)