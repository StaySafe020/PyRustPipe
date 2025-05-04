from pyrustpipe_rules import Rule

rule = Rule("url")
print(rule.validate("https://example.com"))  # Should print True
print(rule.validate("http://test.org"))      # Should print True
print(rule.validate("ftp://bad"))            # Should print False