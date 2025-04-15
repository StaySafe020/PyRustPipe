from pyrustpipe import is_valid_email, is_valid_number
from datetime import datetime
import re

class Rule:
    """A class to define validation rules for data fields."""
    def __init__(self, field_type):
        self.field_type = field_type

    def validate(self, value):
        """Validate a value based on the field type."""
        if self.field_type == "email":
            return is_valid_email(value)
        elif self.field_type == "number":
            return is_valid_number(value)
        elif self.field_type == "date":
            try:
                datetime.strptime(value, "%Y-%m-%d")
                return True
            except (ValueError, TypeError):
                return False
        elif self.field_type == "url":
            pattern = r"^https?://[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(/.*)?$"
            try:
                return bool(re.match(pattern, value, re.IGNORECASE))
            except TypeError:
                return False
        elif self.field_type == "boolean":
            if isinstance(value, bool):
                return True
            return value.lower() in ("true", "false") if isinstance(value, str) else False
        return False