import pytest
from hamcrest import assert_that, equal_to, none

from nodestream.subclass_registry import (
    AlreadyInRegistryError,
    SubclassRegistry,
    MissingFromRegistryError,
)

TEST_REGISTRY = SubclassRegistry()


@TEST_REGISTRY.connect_baseclass
class TestClass:
    pass


class ChildClass(TestClass, name="child"):
    pass


def test_remembers_subclasses_by_name():
    assert_that(TEST_REGISTRY.get("child"), equal_to(ChildClass))
    assert_that(TEST_REGISTRY.name_for(ChildClass), equal_to("child"))


def test_raises_errors_when_invalid_named_subclass():
    with pytest.raises(MissingFromRegistryError):
        TEST_REGISTRY.get("not_there")


def test_raises_errors_when_name_is_resused():
    with pytest.raises(AlreadyInRegistryError):

        class _(TestClass, name="child"):
            pass