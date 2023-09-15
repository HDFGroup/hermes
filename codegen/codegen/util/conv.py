
def str_to_bool(text):
    text = str(text).lower().strip()
    if text == 'false':
        return False
    elif text == 'true':
        return True
    raise Exception("Neither true or false (str_too_bool)")