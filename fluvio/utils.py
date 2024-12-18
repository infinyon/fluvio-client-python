from humanfriendly import parse_size


def parse_byte_size(size: str) -> int:
    # if size cotains "ib" then it is binary size else decimal size
    if "ib" in size.lower():
        return parse_size(size, binary=True)
    return parse_size(size, binary=False)
