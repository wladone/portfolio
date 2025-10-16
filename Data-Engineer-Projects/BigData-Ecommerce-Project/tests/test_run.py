import os
import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

import run


class TestEnv:
    def test_env_with_value(self):
        with patch.dict(os.environ, {'TEST_VAR': 'test_value'}):
            assert run.env('TEST_VAR') == 'test_value'

    def test_env_with_default(self):
        assert run.env('NON_EXISTENT', 'default') == 'default'

    def test_env_required_present(self):
        with patch.dict(os.environ, {'REQUIRED_VAR': 'value'}):
            assert run.env('REQUIRED_VAR', required=True) == 'value'

    def test_env_required_missing(self):
        with pytest.raises(SystemExit):
            run.env('MISSING_VAR', required=True)

    def test_env_empty_string_required(self):
        with patch.dict(os.environ, {'EMPTY_VAR': ''}):
            with pytest.raises(SystemExit):
                run.env('EMPTY_VAR', required=True)


class TestShell:
    @patch('subprocess.run')
    def test_shell_success(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0)
        run.shell(['echo', 'hello'])
        mock_run.assert_called_once_with(['echo', 'hello'], check=True)

    @patch('subprocess.run')
    def test_shell_failure(self, mock_run):
        mock_run.side_effect = RuntimeError('fail')
        with pytest.raises(RuntimeError):
            run.shell(['false'])


class TestRunLocal:
    @patch('run.shell')
    @patch('run.env')
    def test_run_local(self, mock_env, mock_shell):
        mock_env.side_effect = lambda key, default=None, required=False: {
            'PROJECT_ID': 'test-project',
            'REGION': 'us-central1',
            'BIGTABLE_INSTANCE': 'test-instance',
            'BIGTABLE_TABLE': 'test-table',
        }.get(key, default)

        run.run_local()

        expected_args = [
            sys.executable,
            'beam/streaming_pipeline.py',
            '--runner=DirectRunner',
            '--streaming=True',
            '--project=test-project',
            '--region=us-central1',
            'projects/test-project/topics/clicks',
        ]
        called_args = mock_shell.call_args[0][0]
        assert called_args[0:6] == expected_args[0:6]
        assert f"--input_topic_clicks=projects/test-project/topics/clicks" in called_args
        assert f"--dead_letter_topic=projects/test-project/topics/dead-letter" in called_args
        assert f"--output_table_views=product_views_summary" in called_args


class TestRunFlex:
    @patch('run.shell')
    @patch('run.env')
    @patch('run.datetime')
    def test_run_flex(self, mock_datetime, mock_env, mock_shell):
        mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_env.side_effect = lambda key, default=None, required=False: {
            'PROJECT_ID': 'test-project',
            'REGION': 'us-central1',
            'DATAFLOW_BUCKET': 'test-bucket',
            'TEMPLATE_PATH': 'gs://test-bucket/templates/tmpl.json',
            'BIGTABLE_INSTANCE': 'test-instance',
            'BIGTABLE_TABLE': 'test-table',
        }.get(key, default)

        run.run_flex()

        called_args = mock_shell.call_args[0][0]
        assert called_args[:5] == ['gcloud', 'dataflow', 'flex-template', 'run', 'streaming-flex-20230101120000']
        assert '--region' in called_args
        assert '--parameters' in called_args
        params_index = called_args.index('--parameters') + 1
        params = called_args[params_index]
        assert 'project=test-project' in params
        assert 'input_topic_clicks=projects/test-project/topics/clicks' in params
        assert 'dead_letter_topic=projects/test-project/topics/dead-letter' in params


class TestRunDirect:
    @patch('run.shell')
    @patch('run.env')
    def test_run_direct(self, mock_env, mock_shell):
        mock_env.side_effect = lambda key, default=None, required=False: {
            'COMPOSER_ENV': 'test-env',
            'REGION': 'us-central1',
        }.get(key, default)

        run.run_direct()

        called_args = mock_shell.call_args[0][0]
        assert called_args[:5] == ['gcloud', 'composer', 'environments', 'run', 'test-env']
        assert '--location' in called_args
        assert 'dags' in called_args
        assert 'trigger' in called_args
        assert 'streaming_direct_dag' in called_args


class TestMain:
    @patch('run.run_local')
    @patch('sys.argv', ['run.py', 'local'])
    def test_main_local(self, mock_run_local):
        run.main()
        mock_run_local.assert_called_once()

    @patch('run.run_flex')
    @patch('sys.argv', ['run.py', 'flex'])
    def test_main_flex(self, mock_run_flex):
        run.main()
        mock_run_flex.assert_called_once()

    @patch('run.run_direct')
    @patch('sys.argv', ['run.py', 'direct'])
    def test_main_direct(self, mock_run_direct):
        run.main()
        mock_run_direct.assert_called_once()

    @patch('sys.argv', ['run.py'])
    def test_main_default(self):
        with patch('run.run_local') as mock_run_local:
            run.main()
            mock_run_local.assert_called_once()

    @patch('sys.argv', ['run.py', 'invalid'])
    @patch('sys.exit')
    def test_main_invalid_mode(self, mock_exit):
        run.main()
        mock_exit.assert_called_once_with(1)
