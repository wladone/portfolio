"""
Utility functions for the dashboard
"""

import logging
import locale
from typing import Union, Optional, Any
from functools import wraps
import time
import streamlit as st

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def setup_locale() -> None:
    """Setup locale for currency formatting"""
    try:
        locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
    except locale.Error:
        try:
            locale.setlocale(locale.LC_ALL, 'en_US')
        except locale.Error:
            logger.warning("Could not set US locale, using system default")


def format_currency(value: Union[int, float], currency: str = "EUR") -> str:
    """
    Format a numeric value as currency

    Args:
        value: Numeric value to format
        currency: Currency code (default: EUR)

    Returns:
        Formatted currency string
    """
    if not isinstance(value, (int, float)):
        raise ValueError(f"Value must be numeric, got {type(value)}")

    if value < 0:
        logger.warning(f"Negative currency value: {value}")

    try:
        if currency == "EUR":
            # Format as Euro
            if abs(value) >= 100000:
                return f"€{value:,.0f}"
            else:
                return f"€{value:,.2f}"
        else:
            # Generic currency formatting
            return f"{currency} {value:,.2f}"
    except Exception as e:
        logger.error(f"Error formatting currency {value}: {e}")
        return f"{currency} {value}"


def format_number(value: Union[int, float], decimals: int = 0) -> str:
    """
    Format a number with commas and optional decimals

    Args:
        value: Numeric value to format
        decimals: Number of decimal places

    Returns:
        Formatted number string
    """
    if not isinstance(value, (int, float)):
        raise ValueError(f"Value must be numeric, got {type(value)}")

    try:
        if decimals == 0:
            return f"{int(value):,}"
        else:
            return f"{value:,.{decimals}f}"
    except Exception as e:
        logger.error(f"Error formatting number {value}: {e}")
        return str(value)


def format_percent(value: Union[int, float], decimals: int = 1) -> str:
    """
    Format a value as percentage with sign

    Args:
        value: Numeric value to format as percentage
        decimals: Number of decimal places

    Returns:
        Formatted percentage string
    """
    if not isinstance(value, (int, float)):
        raise ValueError(f"Value must be numeric, got {type(value)}")

    try:
        sign = "+" if value >= 0 else ""
        return f"{sign}{value:.{decimals}f}%"
    except Exception as e:
        logger.error(f"Error formatting percentage {value}: {e}")
        return f"{value}%"


def safe_divide(numerator: Union[int, float],
                denominator: Union[int, float],
                default: Union[int, float] = 0) -> float:
    """
    Safely divide two numbers, returning default if denominator is zero

    Args:
        numerator: Number to divide
        denominator: Number to divide by
        default: Default value if division by zero

    Returns:
        Division result or default value
    """
    try:
        if denominator == 0:
            logger.warning(f"Division by zero: {numerator} / {denominator}")
            return default
        return numerator / denominator
    except Exception as e:
        logger.error(f"Error in division {numerator}/{denominator}: {e}")
        return default


def calculate_percentage_change(current: Union[int, float],
                                previous: Union[int, float]) -> float:
    """
    Calculate percentage change between two values

    Args:
        current: Current value
        previous: Previous value

    Returns:
        Percentage change
    """
    if previous == 0:
        return 100.0 if current != 0 else 0.0

    try:
        return ((current - previous) / previous) * 100
    except Exception as e:
        logger.error(
            f"Error calculating percentage change {current}/{previous}: {e}")
        return 0.0


def with_error_handling(func):
    """
    Decorator to add error handling to functions

    Args:
        func: Function to wrap

    Returns:
        Wrapped function with error handling
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}")
            st.error(f"An error occurred: {str(e)}")
            return None
    return wrapper


def with_loading_indicator(message: str = "Loading..."):
    """
    Decorator to show loading indicator during function execution

    Args:
        message: Loading message to display

    Returns:
        Decorator function
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with st.spinner(message):
                start_time = time.time()
                result = func(*args, **kwargs)
                end_time = time.time()
                logger.debug(
                    f"{func.__name__} took {end_time - start_time:.2f}s")
                return result
        return wrapper
    return decorator


def validate_numeric_input(value: Any,
                           min_val: Optional[Union[int, float]] = None,
                           max_val: Optional[Union[int, float]] = None,
                           field_name: str = "value") -> Union[int, float]:
    """
    Validate numeric input with optional range checking

    Args:
        value: Value to validate
        min_val: Minimum allowed value
        max_val: Maximum allowed value
        field_name: Field name for error messages

    Returns:
        Validated numeric value

    Raises:
        ValueError: If validation fails
    """
    if not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be numeric, got {type(value)}")

    if min_val is not None and value < min_val:
        raise ValueError(f"{field_name} must be >= {min_val}, got {value}")

    if max_val is not None and value > max_val:
        raise ValueError(f"{field_name} must be <= {max_val}, got {value}")

    return value


# Initialize locale on import
setup_locale()
