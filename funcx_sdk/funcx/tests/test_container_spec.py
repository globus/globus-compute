from funcx.sdk.container_spec import ContainerSpec


def test_constructor():
    container = ContainerSpec()
    assert not container.pip
    assert not container.apt
    assert not container.conda


def test_to_json():
    container = ContainerSpec(apt=["abc"], pip=["cde==2"], conda=["cccc"])
    assert (
        container.to_json() == '{"apt": ["abc"], "pip": ["cde==2"], "conda": ["cccc"]}'
    )
