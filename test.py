import pyrustpipe
from pyrustpipe_rules import Rule

# Test Rust functions directly
print("Testing Rust functions:")
print(pyrustpipe.say_hello())
print(pyrustpipe.is_valid_email("test@example.com"))
print(pyrustpipe.is_valid_email("invalid.email"))
print(pyrustpipe.is_valid_email("user@domain.co.uk"))

# Test Python Rule class
print("\nTesting Rule class:")
email_rule = Rule("email")
print(email_rule.validate("test@example.com"))  # True
print(email_rule.validate("invalid.email"))     # False
print(email_rule.validate("user@domain.co.uk")) # True
print(email_rule.validate("bad@"))             # False
print(email_rule.validate("me@here.com"))  # True