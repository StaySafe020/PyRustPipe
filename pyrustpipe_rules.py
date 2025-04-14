from pyrustpipe import is_valid_email

class Rule:
    """A class to define validation rules for data fields."""
    def __init__(self, field_type):
        self.field_type = field_type

    def validate(self, value):
        """Validate a value based on the field type."""
        if self.field_type == "email":
            return is_valid_email(value)
        return False