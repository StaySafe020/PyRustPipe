from pyrustpipe_rules import Rule
url_rule = Rule("url")
print(url_rule.validate("HTTPS://Example.COM/path"))  # Should be True
print(url_rule.validate("ftp://bad"))                 # False