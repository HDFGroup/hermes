import re

def to_camel_case(string):
    if string is None:
        return
    words = re.sub(r"(_|-)+", " ", string).split()
    words = [word.capitalize() for word in words]
    return "".join(words)

def to_snake_case(string):
    if string is None:
        return
    string = re.sub('(\.|-)+', '_', string)
    words = re.split('([A-Z][^A-Z]*)', string)
    words = [word for word in words if len(word)]
    string = "_".join(words)
    return string.lower()