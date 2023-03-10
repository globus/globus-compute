import pytest as pytest
from globus_compute_sdk.sdk.container_spec import ContainerSpec


def test_constructor():
    container = ContainerSpec(python_version="3.8.10")
    assert not container.pip
    assert not container.apt
    assert container.conda == ["python=3.8.10"]


def test_constructor_retain_provide_python():
    container = ContainerSpec(conda=["python=3.6"])

    assert container.conda == ["python=3.6"]


def test_bad_python_version():
    with pytest.raises(ValueError):
        _ = ContainerSpec(python_version="3.8.")


def test_to_json():
    container = ContainerSpec(
        apt=["abc"],
        pip=["cde==2"],
        conda=["cccc"],
        description="test_container",
        python_version="3.8",
        payload_url="http://foo.bar",
        name="No Name",
    )
    assert container.to_json() == {
        "name": "No Name",
        "description": "test_container",
        "apt": ["abc"],
        "pip": ["cde==2"],
        "conda": ["cccc", "python=3.8"],
        "payload_url": "http://foo.bar",
    }
