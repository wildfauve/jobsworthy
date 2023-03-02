import re
def to_camel_case(string: str) -> str:
    s = re.sub(r"(_|-)+",
               " ",
               re.sub(r'([A-Z])', r'-\1', string)).title().replace(" ", "")
    return ''.join([s[0].lower(), s[1:]])


def sanitise(string: str) -> str:
    return re.sub(f'(\.)', "_", string)