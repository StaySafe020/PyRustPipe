import pyrustpipe
import time
import subprocess
import sys
from pyrustpipe_rules import Rule

print("Testing basic functions:")
print("say_hello:", pyrustpipe.say_hello())
print("is_valid_email('test@example.com'):", pyrustpipe.is_valid_email("test@example.com"))
print("is_valid_email('invalid'):", pyrustpipe.is_valid_email("invalid"))
print("is_valid_number('123.45'):", pyrustpipe.is_valid_number("123.45"))
print("is_valid_number('abc'):", pyrustpipe.is_valid_number("abc"))

print("\nTesting URL normalization:")
print("normalize_url('HTTPS://Example.COM/path'):", pyrustpipe.normalize_url("HTTPS://Example.COM/path"))
print("normalize_url('ftp://example.com'):", pyrustpipe.normalize_url("ftp://example.com"))

print("\nTesting URL validation:")
rule = Rule("url")
print("Rule('url').validate('https://example.com'):", rule.validate("https://example.com"))
print("Rule('url').validate('ftp://example.com'):", rule.validate("ftp://example.com"))

print("\nTesting stream processing:")
with open("stream.json") as f:
    lines = [line.strip() for line in f if line.strip()]
print("Email pipeline:")
results = pyrustpipe.process_json_stream(lines, "email", "email")
print("Results:", results)
print("Number pipeline:")
results = pyrustpipe.process_json_stream(lines, "number", "number")
print("Results:", results)
print("Url pipeline:")
results = pyrustpipe.process_json_stream(lines, "url", "url")
print("Results:", results)
print("Boolean pipeline:")
results = pyrustpipe.process_json_stream(lines, "boolean", "boolean")
print("Results:", results)

print("\nRunning remaining tests in subprocess...")
subprocess.run([sys.executable, "-c", """
import pyrustpipe
import time
with open('stream.json') as f:
    lines = [line.strip() for line in f if line.strip()]
print('\\nTesting stream with errors:')
results, errors = pyrustpipe.process_json_stream_with_errors(lines, 'email', 'email')
print('Email results:', results)
print('Errors:', errors)

print('\\nTesting batch processing:')
field_types = [('email', 'email'), ('number', 'number'), ('url', 'url'), ('boolean', 'boolean')]
batch_results = pyrustpipe.process_json_stream_batch(lines, field_types)
print('Batch results:', batch_results)

print('\\nTesting file processing:')
results = pyrustpipe.process_json_file('large.json', 'email', 'email')
print(f'File email pipeline: {len(results)} emails')

print('\\nTesting stream iterator:')
with open('stream.json') as f:
    results = pyrustpipe.process_json_stream_iter(f, 'url', 'url')
    print(f'Stream URL pipeline: {results}')

print('\\nBenchmarking:')
start = time.time()
with open('large.json') as f:
    lines = [line.strip() for line in f if line.strip()]
    print(f'Processing {len(lines)} lines from large.json')
    results = pyrustpipe.process_json_stream(lines, 'email', 'email')
print(f'Email pipeline: {len(results)} items in {time.time() - start:.2f}s')
"""])