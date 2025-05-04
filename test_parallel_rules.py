from pyrustpipe_rules import Rule
from concurrent.futures import ThreadPoolExecutor

urls = ["HTTPS://Example.COM/path", "ftp://bad", "HTTP://TEST.ORG/path"]

def validate_url(url):
    rule = Rule("url")
    result = rule.validate(url)
    print(f"Validated {url}: {result}")
    return result

with ThreadPoolExecutor() as executor:
    results = executor.map(validate_url, urls)

print("Results:", list(results))