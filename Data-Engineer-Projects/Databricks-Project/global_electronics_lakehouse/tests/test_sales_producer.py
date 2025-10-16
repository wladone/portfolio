"""Unit tests for sales_producer.py"""
import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from kafka import KafkaProducer

from src.streaming.sales_producer import (
    load_events,
    build_producer,
    stream_events,
    parse_args,
    main,
)


class TestLoadEvents:
    """Test load_events function."""

    def test_load_events_success(self):
        """Test successful loading of events from JSON files."""
        events_data = [
            {"event_id": "EVT001", "order_id": "ORD001"},
            {"event_id": "EVT002", "order_id": "ORD002"}
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            data_dir = Path(temp_dir)
            json_file = data_dir / "sales_001.json"
            with json_file.open('w') as f:
                for event in events_data:
                    f.write(json.dumps(event) + '\n')

            events = load_events(data_dir)

            assert len(events) == 2
            assert events[0]["event_id"] == "EVT001"

    def test_load_events_no_files(self):
        """Test load_events with no JSON files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_dir = Path(temp_dir)

            with pytest.raises(RuntimeError, match="No events found"):
                load_events(data_dir)

    def test_load_events_invalid_json(self):
        """Test load_events with invalid JSON."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_dir = Path(temp_dir)
            json_file = data_dir / "sales_001.json"
            with json_file.open('w') as f:
                f.write("invalid json line\n")

            with pytest.raises(ValueError, match="Invalid JSON"):
                load_events(data_dir)

    def test_load_events_nonexistent_directory(self):
        """Test load_events with nonexistent directory."""
        nonexistent_path = Path("/nonexistent/path")

        with pytest.raises(RuntimeError, match="Data path does not exist"):
            load_events(nonexistent_path)


class TestBuildProducer:
    """Test build_producer function."""

    @patch('src.streaming.sales_producer.KafkaProducer')
    def test_build_producer_success(self, mock_kafka_producer):
        """Test successful creation of Kafka producer."""
        mock_producer = MagicMock()
        mock_kafka_producer.return_value = mock_producer

        producer = build_producer("localhost:9092")

        assert producer == mock_producer
        mock_kafka_producer.assert_called_once()

    @patch('src.streaming.sales_producer.KafkaProducer')
    def test_build_producer_failure(self, mock_kafka_producer):
        """Test build_producer with failure."""
        mock_kafka_producer.side_effect = Exception("Connection failed")

        with pytest.raises(RuntimeError, match="Failed to create Kafka producer"):
            build_producer("localhost:9092")


class TestStreamEvents:
    """Test stream_events function."""

    @patch('src.streaming.sales_producer.time.sleep')
    def test_stream_events_no_replay(self, mock_sleep):
        """Test stream_events without replay."""
        mock_producer = MagicMock()
        events = [{"event_id": "EVT001", "order_id": "ORD001"}]
        mock_logger = MagicMock()

        stream_events(mock_producer, "test_topic",
                      events, 10, False, mock_logger)

        mock_producer.send.assert_called()
        mock_producer.flush.assert_called()

    @patch('src.streaming.sales_producer.time.sleep')
    def test_stream_events_with_replay(self, mock_sleep):
        """Test stream_events with replay."""
        mock_producer = MagicMock()
        events = [{"event_id": "EVT001", "order_id": "ORD001"}]
        mock_logger = MagicMock()

        stream_events(mock_producer, "test_topic",
                      events, 10, True, mock_logger)

        # Should call send multiple times due to replay
        assert mock_producer.send.call_count > 1

    @patch('src.streaming.sales_producer.time.sleep')
    def test_stream_events_keyboard_interrupt(self, mock_sleep):
        """Test stream_events with keyboard interrupt."""
        mock_producer = MagicMock()
        mock_producer.send.side_effect = KeyboardInterrupt()
        events = [{"event_id": "EVT001", "order_id": "ORD001"}]
        mock_logger = MagicMock()

        stream_events(mock_producer, "test_topic",
                      events, 10, False, mock_logger)

        mock_logger.info.assert_called_with(
            "Interrupted by user, flushing pending messages...")

    def test_stream_events_rate_zero(self):
        """Test stream_events with rate 0 (max speed)."""
        mock_producer = MagicMock()
        events = [{"event_id": "EVT001", "order_id": "ORD001"}]
        mock_logger = MagicMock()

        stream_events(mock_producer, "test_topic",
                      events, 0, False, mock_logger)

        mock_producer.send.assert_called()


class TestParseArgs:
    """Test parse_args function."""

    def test_parse_args_default(self):
        """Test parse_args with default values."""
        args = parse_args([])

        assert args.bootstrap == "localhost:9092"
        assert args.topic == "global_electronics_sales"
        assert args.rate == 25
        assert not args.replay

    def test_parse_args_custom(self):
        """Test parse_args with custom values."""
        argv = [
            "--bootstrap", "kafka:9092",
            "--topic", "test_topic",
            "--rate", "50",
            "--replay"
        ]
        args = parse_args(argv)

        assert args.bootstrap == "kafka:9092"
        assert args.topic == "test_topic"
        assert args.rate == 50
        assert args.replay

    def test_parse_args_data_path(self):
        """Test parse_args with custom data path."""
        argv = ["--data-path", "/custom/path"]
        args = parse_args(argv)

        assert str(args.data_path) == "/custom/path"


class TestMainFunction:
    """Test main function."""

    @patch('src.streaming.sales_producer.parse_args')
    @patch('src.streaming.sales_producer.load_events')
    @patch('src.streaming.sales_producer.build_producer')
    @patch('src.streaming.sales_producer.stream_events')
    def test_main_success(self, mock_stream, mock_build_producer, mock_load_events, mock_parse_args):
        """Test main function success path."""
        mock_args = MagicMock()
        mock_args.bootstrap = "localhost:9092"
        mock_args.topic = "test_topic"
        mock_args.rate = 25
        mock_args.replay = False
        mock_parse_args.return_value = mock_args

        mock_events = [{"event_id": "EVT001"}]
        mock_load_events.return_value = mock_events

        mock_producer = MagicMock()
        mock_build_producer.return_value = mock_producer

        result = main([])

        assert result == 0
        mock_load_events.assert_called_once()
        mock_build_producer.assert_called_once()
        mock_stream.assert_called_once()

    @patch('src.streaming.sales_producer.parse_args')
    def test_main_invalid_rate(self, mock_parse_args):
        """Test main function with invalid rate."""
        mock_args = MagicMock()
        mock_args.rate = -1
        mock_parse_args.return_value = mock_args

        result = main([])

        assert result == 1

    @patch('src.streaming.sales_producer.parse_args')
    def test_main_missing_bootstrap(self, mock_parse_args):
        """Test main function with missing bootstrap servers."""
        mock_args = MagicMock()
        mock_args.rate = 25
        mock_args.bootstrap = ""
        mock_parse_args.return_value = mock_args

        result = main([])

        assert result == 1

    @patch('src.streaming.sales_producer.parse_args')
    def test_main_missing_topic(self, mock_parse_args):
        """Test main function with missing topic."""
        mock_args = MagicMock()
        mock_args.rate = 25
        mock_args.bootstrap = "localhost:9092"
        mock_args.topic = ""
        mock_parse_args.return_value = mock_args

        result = main([])

        assert result == 1

    @patch('src.streaming.sales_producer.parse_args')
    @patch('src.streaming.sales_producer.load_events')
    def test_main_load_events_failure(self, mock_load_events, mock_parse_args):
        """Test main function when load_events fails."""
        mock_args = MagicMock()
        mock_args.bootstrap = "localhost:9092"
        mock_args.topic = "test_topic"
        mock_args.rate = 25
        mock_parse_args.return_value = mock_args

        mock_load_events.side_effect = Exception("Load failed")

        result = main([])

        assert result == 1


if __name__ == "__main__":
    pytest.main([__file__])
