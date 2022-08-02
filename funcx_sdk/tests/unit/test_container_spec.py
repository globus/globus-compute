from funcx.sdk.container_spec import ContainerSpec


def test_constructor():
    container = ContainerSpec()
    assert not container.pip
    assert not container.apt
    assert not container.conda


def test_to_json():
    container = ContainerSpec(
        apt=["abc"],
        pip=["cde==2"],
        conda=["cccc"],
        description="test_container",
        payload_url="http://foo.bar",
        name="No Name",
    )
    assert container.to_json() == {
        "name": "No Name",
        "description": "test_container",
        "apt": ["abc"],
        "pip": ["cde==2"],
        "conda": ["cccc"],
        "payload_url": "http://foo.bar",
    }
