import pyrustpipe
from pyrustpipe_rules import Rule
import json

# Test Rust functions directly
print("Testing Rust functions:")
print(pyrustpipe.say_hello())
print(pyrustpipe.is_valid_email("test@example.com"))
print(pyrustpipe.is_valid_email("invalid.email"))
print(pyrustpipe.is_valid_number("123.45"))
print(pyrustpipe.is_valid_number("abc"))

# Test Python Rule class
print("\nTesting Rule class:")
email_rule = Rule("email")
print(email_rule.validate("test@example.com"))
print(email_rule.validate("invalid.email"))
number_rule = Rule("number")
print(number_rule.validate("123.45"))
print(number_rule.validate("abc"))
date_rule = Rule("date")
print(date_rule.validate("2025-04-15"))
print(date_rule.validate("2025-13-01"))
url_rule = Rule("url")
print(url_rule.validate("https://example.com"))
print(url_rule.validate("ftp://bad"))
bool_rule = Rule("boolean")
print(bool_rule.validate("true"))
print(bool_rule.validate("maybe"))
print(bool_rule.validate(True))

# Test JSON parsing
print("\nTesting JSON parsing:")
print(pyrustpipe.validate_json_field('{"email": "test@example.com"}', "email", "email"))
print(pyrustpipe.validate_json_field('{"num": "123.45"}', "num", "number"))
try:
    pyrustpipe.validate_json_field('{"wrong": "123.45"}', "wrong", "number")
except ValueError as e:
    print(f"Expected error: {e}")

# Test JSON file
print("\nTesting JSON file:")
with open("test.json") as f:
    data = json.load(f)
    print(pyrustpipe.validate_json_field(json.dumps(data), "email", "email"))
    print(pyrustpipe.validate_json_field(json.dumps(data), "number", "number"))

# Test JSON stream
print("\nTesting JSON stream:")
with open("stream.json") as f:
    lines = [line.strip() for line in f.readlines() if line.strip()]
    for field_type in ["email", "number", "url", "boolean"]:
        results = pyrustpipe.validate_json_stream(lines, field_type, field_type)
        print(f"{field_type.capitalize()} stream:", results)

# Test pipeline chaining
print("\nTesting pipeline chaining:")
with open("stream.json") as f:
    lines = [line.strip() for line in f.readlines() if line.strip()]
    for field_type in ["email", "number", "url", "boolean"]:
        results = pyrustpipe.process_json_stream(lines, field_type, field_type)
        print(f"{field_type.capitalize()} pipeline:", results)

# Test pipeline with error reporting
print("\nTesting pipeline with error reporting:")
with open("stream.json") as f:
    lines = [line.strip() for line in f.readlines() if line.strip()]
    for field_type in ["email", "number", "url", "boolean"]:
        results, errors = pyrustpipe.process_json_stream_with_errors(lines, field_type, field_type)
        print(f"{field_type.capitalize()} pipeline:", results)
        print(f"{field_type.capitalize()} errors:", errors)

# Test batch processing
print("\nTesting batch processing:")
with open("stream.json") as f:
    lines = [line.strip() for line in f.readlines() if line.strip()]
    field_types = [("email", "email"), ("number", "number"), ("url", "url"), ("boolean", "boolean")]
    results = pyrustpipe.process_json_stream_batch(lines, field_types)
    print("Batch results:", results)