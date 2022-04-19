import random
import string

import pytest


@pytest.fixture
def randomstring():
    def func(length=5, alphabet=string.ascii_letters):
        return "".join(random.choice(alphabet) for _ in range(length))

    return func
