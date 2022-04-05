import random as _random
import string as _string


def randomstring(length=5, alphabet=_string.ascii_letters):
    return "".join(_random.choice(alphabet) for _ in range(length))
