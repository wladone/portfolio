#!/usr/bin/env python3
"""
Data Quality Validation Module for Streaming Pipeline

This module provides comprehensive data quality validation including:
- Schema validation with detailed error reporting
- Business rule validation
- Duplicate detection
- Data completeness checks
- Data lineage tracking

Usage:
    from beam.data_quality import DataQualityValidator, BusinessRulesValidator

    validator = DataQualityValidator()
    business_validator = BusinessRulesValidator()

    # Use in pipeline
    validated_data = (events
                    | beam.Map(validator.validate_schema)
                    | beam.Map(business_validator.validate_business_rules))
"""

import json
import re
import hashlib
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass

import apache_beam as beam
from apache_beam import DoFn, ParDo, Map


@dataclass
class ValidationError:
    """Represents a data validation error."""
    rule_name: str
    error_type: str  # "schema", "business_rule", "duplicate", "completeness"
    field: str
    value: Any
    message: str
    severity: str = "error"  # "error", "warning", "info"
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now(timezone.utc)


@dataclass
class DataQualityMetrics:
    """Tracks data quality metrics."""
    total_events: int = 0
    valid_events: int = 0
    invalid_events: int = 0
    duplicate_events: int = 0
    missing_data_events: int = 0
    business_rule_violations: int = 0

    def record_event_validation(self, is_valid: bool, has_duplicates: bool = False,
                                missing_data: bool = False, business_rules_ok: bool = True):
        """Record validation results for an event."""
        self.total_events += 1
        if is_valid and not has_duplicates and not missing_data and business_rules_ok:
            self.valid_events += 1
        else:
            self.invalid_events += 1
            if has_duplicates:
                self.duplicate_events += 1
            if missing_data:
                self.missing_data_events += 1
            if not business_rules_ok:
                self.business_rule_violations += 1


class DataQualityValidator:
    """Comprehensive data quality validator."""

    def __init__(self):
        self.metrics = DataQualityMetrics()

        # Schema definitions for each event type
        self.schemas = {
            "click": {
                "required_fields": ["event_type", "product_id", "user_id", "timestamp"],
                "field_types": {
                    "event_type": str,
                    "product_id": str,
                    "user_id": str,
                    "session_id": str,
                    "page_type": str,
                    "user_agent": str,
                    "timestamp": str
                },
                "field_patterns": {
                    "product_id": r"^product_[a-zA-Z0-9]+$",
                    "user_id": r"^user_[a-zA-Z0-9]+$",
                    "session_id": r"^session_[a-zA-Z0-9]+$",
                    "page_type": r"^(product|category|search|home)$",
                    "timestamp": r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"
                }
            },
            "transaction": {
                "required_fields": ["event_type", "product_id", "store_id", "quantity", "unit_price", "timestamp"],
                "field_types": {
                    "event_type": str,
                    "product_id": str,
                    "store_id": str,
                    "quantity": int,
                    "unit_price": (int, float),
                    "total_amount": (int, float),
                    "currency": str,
                    "payment_method": str,
                    "timestamp": str
                },
                "field_patterns": {
                    "product_id": r"^product_[a-zA-Z0-9]+$",
                    "store_id": r"^store_[a-zA-Z0-9]+$",
                    "currency": r"^[A-Z]{3}$",
                    "payment_method": r"^(credit_card|paypal|apple_pay|google_pay)$",
                    "timestamp": r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"
                },
                "field_ranges": {
                    "quantity": (1, 1000),
                    "unit_price": (0.01, 10000.00)
                }
            },
            "stock": {
                "required_fields": ["event_type", "product_id", "warehouse_id", "quantity_change", "timestamp"],
                "field_types": {
                    "event_type": str,
                    "product_id": str,
                    "warehouse_id": str,
                    "quantity_change": int,
                    "reason": str,
                    "timestamp": str
                },
                "field_patterns": {
                    "product_id": r"^product_[a-zA-Z0-9]+$",
                    "warehouse_id": r"^warehouse_[a-zA-Z0-9]+$",
                    "reason": r"^(restock|sale|return|adjustment|damaged|transfer)$",
                    "timestamp": r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"
                },
                "field_ranges": {
                    "quantity_change": (-10000, 10000)
                }
            }
        }

    def validate_schema(self, event: Dict[str, Any]) -> Tuple[Dict[str, Any], List[ValidationError]]:
        """Validate event against schema."""
        errors = []
        event_type = event.get("event_type")

        if event_type not in self.schemas:
            errors.append(ValidationError(
                rule_name="valid_event_type",
                error_type="schema",
                field="event_type",
                value=event_type,
                message=f"Unknown event type: {event_type}"
            ))
            return event, errors

        schema = self.schemas[event_type]

        # Check required fields
        for field in schema["required_fields"]:
            if field not in event or event[field] is None or str(event[field]).strip() == "":
                errors.append(ValidationError(
                    rule_name="required_field",
                    error_type="schema",
                    field=field,
                    value=event.get(field),
                    message=f"Required field '{field}' is missing or empty"
                ))

        # Check field types
        for field, expected_type in schema["field_types"].items():
            if field in event and event[field] is not None:
                if not isinstance(event[field], expected_type):
                    errors.append(ValidationError(
                        rule_name="field_type",
                        error_type="schema",
                        field=field,
                        value=event[field],
                        message=f"Field '{field}' should be {expected_type.__name__}, got {type(event[field]).__name__}"
                    ))

        # Check field patterns
        for field, pattern in schema.get("field_patterns", {}).items():
            if field in event and event[field] is not None:
                if not re.match(pattern, str(event[field])):
                    errors.append(ValidationError(
                        rule_name="field_pattern",
                        error_type="schema",
                        field=field,
                        value=event[field],
                        message=f"Field '{field}' does not match pattern: {pattern}"
                    ))

        # Check field ranges
        for field, (min_val, max_val) in schema.get("field_ranges", {}).items():
            if field in event and event[field] is not None:
                value = event[field]
                if not (min_val <= value <= max_val):
                    errors.append(ValidationError(
                        rule_name="field_range",
                        error_type="schema",
                        field=field,
                        value=value,
                        message=f"Field '{field}' value {value} is outside range [{min_val}, {max_val}]"
                    ))

        return event, errors

    def validate_completeness(self, event: Dict[str, Any]) -> List[ValidationError]:
        """Validate data completeness."""
        errors = []
        event_type = event.get("event_type", "")

        # Check for suspiciously short or empty values
        for field, value in event.items():
            if isinstance(value, str) and len(str(value).strip()) < 2:
                errors.append(ValidationError(
                    rule_name="data_completeness",
                    error_type="completeness",
                    field=field,
                    value=value,
                    message=f"Field '{field}' has suspiciously short value: '{value}'",
                    severity="warning"
                ))

        # Check for missing optional but important fields
        important_fields = {
            "click": ["session_id", "page_type"],
            "transaction": ["currency", "payment_method"],
            "stock": ["reason"]
        }

        if event_type in important_fields:
            for field in important_fields[event_type]:
                if field not in event or not event[field]:
                    errors.append(ValidationError(
                        rule_name="important_field_missing",
                        error_type="completeness",
                        field=field,
                        value=None,
                        message=f"Important field '{field}' is missing",
                        severity="warning"
                    ))

        return errors

    def detect_duplicates(self, event: Dict[str, Any], window_events: List[Dict[str, Any]]) -> List[ValidationError]:
        """Detect duplicate events within a time window."""
        errors = []

        # Create a hash of key fields for duplicate detection
        key_fields = {
            "click": ["user_id", "product_id", "timestamp"],
            "transaction": ["transaction_id"],
            "stock": ["product_id", "warehouse_id", "timestamp"]
        }

        event_type = event.get("event_type")
        if event_type in key_fields:
            # Create duplicate detection key
            key_values = [str(event.get(field, ""))
                          for field in key_fields[event_type]]
            duplicate_key = hashlib.md5(
                "|".join(key_values).encode()).hexdigest()

            # Check for duplicates in current window
            duplicate_count = sum(
                1 for e in window_events
                if self._get_duplicate_key(e, key_fields[event_type]) == duplicate_key
            )

            if duplicate_count > 1:
                errors.append(ValidationError(
                    rule_name="duplicate_detection",
                    error_type="duplicate",
                    field="event",
                    value=duplicate_key,
                    message=f"Duplicate event detected (count: {duplicate_count})",
                    severity="error"
                ))

        return errors

    def _get_duplicate_key(self, event: Dict[str, Any], key_fields: List[str]) -> str:
        """Generate duplicate detection key for an event."""
        key_values = [str(event.get(field, "")) for field in key_fields]
        return hashlib.md5("|".join(key_values).encode()).hexdigest()


class BusinessRulesValidator:
    """Validates business rules and logic."""

    def __init__(self):
        self.metrics = DataQualityMetrics()

    def validate_business_rules(self, event: Dict[str, Any]) -> List[ValidationError]:
        """Validate business rules for events."""
        errors = []
        event_type = event.get("event_type")

        if event_type == "transaction":
            errors.extend(self._validate_transaction_rules(event))
        elif event_type == "stock":
            errors.extend(self._validate_stock_rules(event))
        elif event_type == "click":
            errors.extend(self._validate_click_rules(event))

        return errors

    def _validate_transaction_rules(self, event: Dict[str, Any]) -> List[ValidationError]:
        """Validate business rules for transactions."""
        errors = []

        # Rule: Total amount should equal quantity * unit_price
        quantity = event.get("quantity", 0)
        unit_price = event.get("unit_price", 0)
        total_amount = event.get("total_amount", 0)

        expected_total = round(quantity * unit_price, 2)
        if abs(total_amount - expected_total) > 0.01:  # Allow for small rounding errors
            errors.append(ValidationError(
                rule_name="transaction_amount_calculation",
                error_type="business_rule",
                field="total_amount",
                value=total_amount,
                message=f"Total amount {total_amount} doesn't match quantity * unit_price ({quantity} * {unit_price} = {expected_total})"
            ))

        # Rule: High-value transactions should have valid payment methods
        if total_amount > 1000 and event.get("payment_method") not in ["credit_card"]:
            errors.append(ValidationError(
                rule_name="high_value_transaction_payment",
                error_type="business_rule",
                field="payment_method",
                value=event.get("payment_method"),
                message=f"High-value transaction (${total_amount}) should use credit_card payment",
                severity="warning"
            ))

        return errors

    def _validate_stock_rules(self, event: Dict[str, Any]) -> List[ValidationError]:
        """Validate business rules for stock events."""
        errors = []

        # Rule: Large quantity changes should have valid reasons
        quantity_change = abs(event.get("quantity_change", 0))
        reason = event.get("reason", "")

        if quantity_change > 1000:
            valid_reasons = ["restock", "adjustment"]
            if reason not in valid_reasons:
                errors.append(ValidationError(
                    rule_name="large_stock_change_reason",
                    error_type="business_rule",
                    field="reason",
                    value=reason,
                    message=f"Large stock change ({quantity_change}) should have reason 'restock' or 'adjustment'",
                    severity="warning"
                ))

        # Rule: Negative stock changes should be sales or damaged
        if event.get("quantity_change", 0) < 0:
            valid_negative_reasons = ["sale", "damaged", "return"]
            if reason not in valid_negative_reasons:
                errors.append(ValidationError(
                    rule_name="negative_stock_reason",
                    error_type="business_rule",
                    field="reason",
                    value=reason,
                    message=f"Negative stock change should have reason: {valid_negative_reasons}",
                    severity="warning"
                ))

        return errors

    def _validate_click_rules(self, event: Dict[str, Any]) -> List[ValidationError]:
        """Validate business rules for click events."""
        errors = []

        # Rule: Product views should have reasonable session patterns
        session_id = event.get("session_id", "")
        if session_id:
            # Extract session number
            try:
                session_num = int(session_id.replace("session_", ""))
                if session_num < 1 or session_num > 1000000:
                    errors.append(ValidationError(
                        rule_name="session_id_range",
                        error_type="business_rule",
                        field="session_id",
                        value=session_id,
                        message="Session ID seems out of reasonable range",
                        severity="warning"
                    ))
            except ValueError:
                pass  # Not a numeric session ID

        return errors


class DataQualityDoFn(DoFn):
    """Apache Beam DoFn for data quality validation."""

    def __init__(self):
        self.validator = DataQualityValidator()
        self.business_validator = BusinessRulesValidator()

    def process(self, event: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process and validate an event."""
        # Update metrics
        self.validator.metrics.record_event_validation(True)

        # Schema validation
        validated_event, schema_errors = self.validator.validate_schema(event)

        # Completeness validation
        completeness_errors = self.validator.validate_completeness(
            validated_event)

        # Business rules validation
        business_errors = self.business_validator.validate_business_rules(
            validated_event)

        # Combine all errors
        all_errors = schema_errors + completeness_errors + business_errors

        if all_errors:
            # Log errors for monitoring
            for error in all_errors:
                print(f"Data Quality Error: {error}")

            # Return event with error information
            return [{
                "event": validated_event,
                "validation_errors": [self._error_to_dict(error) for error in all_errors],
                "is_valid": False
            }]
        else:
            return [{
                "event": validated_event,
                "validation_errors": [],
                "is_valid": True
            }]

    def _error_to_dict(self, error: ValidationError) -> Dict[str, Any]:
        """Convert ValidationError to dictionary."""
        return {
            "rule_name": error.rule_name,
            "error_type": error.error_type,
            "field": error.field,
            "value": error.value,
            "message": error.message,
            "severity": error.severity,
            "timestamp": error.timestamp.isoformat()
        }


def create_data_quality_transforms():
    """Create Apache Beam transforms for data quality validation."""

    # Schema validation transform
    schema_validator = DataQualityValidator()

    class ValidateSchemaDoFn(DoFn):
        def process(self, event):
            validated_event, errors = schema_validator.validate_schema(event)
            if errors:
                # Route to dead letter
                yield beam.pvalue.TaggedOutput('dead_letter', {
                    'event': event,
                    'errors': [error.message for error in errors]
                })
            else:
                yield validated_event

    # Business rules validation transform
    business_validator = BusinessRulesValidator()

    class ValidateBusinessRulesDoFn(DoFn):
        def process(self, event):
            errors = business_validator.validate_business_rules(event)
            if errors:
                # Log warnings but don't fail the event
                for error in errors:
                    if error.severity == "error":
                        yield beam.pvalue.TaggedOutput('invalid', event)
                    else:
                        print(f"Business Rule Warning: {error.message}")

            yield event

    return {
        'validate_schema': ValidateSchemaDoFn,
        'validate_business_rules': ValidateBusinessRulesDoFn,
        'comprehensive_validation': DataQualityDoFn
    }


# Utility functions for data lineage tracking
def track_data_lineage(event: Dict[str, Any], source_topic: str, processing_step: str) -> Dict[str, Any]:
    """Add data lineage information to an event."""
    lineage_info = {
        "data_lineage": {
            "source_topic": source_topic,
            "processing_step": processing_step,
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "pipeline_version": "1.0.0",
            "event_hash": hashlib.md5(json.dumps(event, sort_keys=True).encode()).hexdigest()
        }
    }

    # Merge with existing event
    event_copy = event.copy()
    event_copy.update(lineage_info)
    return event_copy


def validate_data_quality_standalone(event: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Standalone data quality validation function."""
    validator = DataQualityValidator()
    business_validator = BusinessRulesValidator()

    # Schema validation
    _, schema_errors = validator.validate_schema(event)

    # Business rules validation
    business_errors = business_validator.validate_business_rules(event)

    # Combine errors
    all_errors = schema_errors + business_errors

    error_messages = [error.message for error in all_errors]
    is_valid = len(all_errors) == 0

    return is_valid, error_messages
