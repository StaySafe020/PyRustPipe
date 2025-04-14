import pyrustpipe

print(pyrustpipe.say_hello())
print(pyrustpipe.is_valid_email("test@example.com"))
print(pyrustpipe.is_valid_email("invalid.email"))
print(pyrustpipe.is_valid_email("user@domain.co.uk"))
print(pyrustpipe.is_valid_email("bad@"))
print(pyrustpipe.is_valid_email("me@here.com"))