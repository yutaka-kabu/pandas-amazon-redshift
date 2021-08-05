def to_utf8_bytes(text):
    try:
        return text.encode("utf-8", "strict")
    except UnicodeError as err:
        raise TypeError(
            "'{}' is cannot be converted to UTF-8".format(text)
        ) from err
