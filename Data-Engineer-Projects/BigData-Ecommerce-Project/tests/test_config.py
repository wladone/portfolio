import pytest

import config


def test_validate_configuration_defaults():
    # Should not raise for default configuration values.
    config.validate_configuration()


def test_validate_bigquery_targets_rejects_bad_names():
    with pytest.raises(ValueError):
        config.validate_bigquery_targets("bad dataset", [config.VIEWS_TABLE])
    with pytest.raises(ValueError):
        config.validate_bigquery_targets(config.DATASET_NAME, ["bad-table!"])


def test_validate_pubsub_topics_rejects_illegal_chars():
    with pytest.raises(ValueError):
        config.validate_pubsub_topics(["topic with space"])


def test_validate_bigtable_rejects_invalid_table():
    with pytest.raises(ValueError):
        config.validate_bigtable("table name with space")



def test_validate_configuration_invalid_project(monkeypatch):
    monkeypatch.setattr(config, 'PROJECT_ID', 'bad id')
    with pytest.raises(ValueError):
        config.validate_configuration()


def test_validate_bigtable_invalid_column_family():
    with pytest.raises(ValueError):
        config.validate_bigtable('valid-table', 'bad family !')
