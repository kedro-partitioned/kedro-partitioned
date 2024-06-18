"""Fake Dataset for testing."""

from __future__ import annotations
from typing import Any, Dict
from kedro.io import AbstractDataset
import pandas as pd


class MockedDataset(AbstractDataset):
    """Fake Dataset for testing."""

    EXAMPLE_DATA = pd.DataFrame({"fruits": ["Apple", "Pear"], "price": [10, 15]})

    dfs = {}

    def __init__(self, *args: Any, **kwargs: Any):
        """Initialize a MockedDataset.

        Args:
            args: Positional arguments.
            kwargs: Keyword arguments.
        """
        self.args = args
        self.kwargs = kwargs

    def from_config(self, *args: Any, **kwargs: Any) -> MockedDataset:
        """Create a new instance from a config dict.

        Returns:
            MockedDataset: New instance.
        """
        return MockedDataset()

    def _save(self, df: Any, *args: Any, **kwargs: Any):
        self.dfs[self.kwargs["filepath"]] = df

    def _load(self, *args: Any, **kwargs: Any) -> Any:
        if self.kwargs["filepath"] in self.dfs:
            return self.dfs[self.kwargs["filepath"]]
        else:
            return self.EXAMPLE_DATA

    def _describe(self) -> Dict[str, Any]:
        return "Described"
