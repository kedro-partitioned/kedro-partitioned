"""Threaded partitioned dataset tests."""

import pathlib
from typing import List
import pytest
from pytest_mock import MockFixture
from .mocked_dataset import MockedDataset
from kedro_partitioned.extras.datasets.threaded_partitioned_dataset import (
    ThreadedPartitionedDataset,
)


BASE_PATH = pathlib.Path.cwd() / "a"


@pytest.fixture(autouse=True)
def mock_load(mocker: MockFixture):
    """Overwrites ThreadedPartitionedDataset load.

    Args:
        mocker (MockFixture): pytest-mock fixture
    """

    def _list_partitions(self: ThreadedPartitionedDataset) -> List[str]:
        return [(BASE_PATH / "a" / sp).as_posix() for sp in ["a", "b", "c"]]

    mocker.patch.object(
        ThreadedPartitionedDataset, "_list_partitions", _list_partitions
    )


@pytest.fixture()
def setup() -> ThreadedPartitionedDataset:
    """Returns ThreadedPartitionedDataset instance.

    Returns:
        ThreadedPartitionedDataset: threaded partitioned dataset
    """
    return ThreadedPartitionedDataset(
        path=(BASE_PATH / "a").as_posix(), dataset=MockedDataset
    )


def test_save(setup: ThreadedPartitionedDataset):
    """Test save method.

    Args:
        setup (ThreadedPartitionedDataset): threaded partitioned dataset
    """
    to_save = {
        path: loader().assign(cnt=i)
        for i, (path, loader) in enumerate(setup.load().items())
    }
    setup.save(to_save)
    assert all(["cnt" in loader() for loader in setup.load().values()])


def test_save_callable(setup: ThreadedPartitionedDataset):
    """Test save method with callable.

    Args:
        setup (ThreadedPartitionedDataset): threaded partitioned dataset
    """
    to_save = {
        path: lambda: loader().assign(cnt=i)
        for i, (path, loader) in enumerate(setup.load().items())
    }
    setup.save(to_save)
    assert all(["cnt" in loader() for loader in setup.load().values()])
