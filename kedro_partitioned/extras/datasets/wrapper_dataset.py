"""A Dataset that wraps another for overloading."""

from kedro.io import AbstractDataset
from typing import Any, Dict, Type


class WrapperDataset(AbstractDataset):
    """Proxies methods of a specified Dataset.

    Example:
        >>> from kedro.io import MemoryDataset

        All kwargs are passed to the wrapped Dataset

        >>> d = WrapperDataset(dataset=MemoryDataset, data=1)
        >>> d.load()
        1
        >>> d.save(3)
        >>> d.load()
        3
        >>> d._describe()
        {'data': '<int>'}
        >>> d.exists()
        True
        >>> d.release()
    """

    def __init__(self, dataset: Type[AbstractDataset], **kwargs: Any):
        """Initialize a new WrapperDataset.

        Args:
            dataset (Type[AbstractDataset]): The Dataset to wrap.
        """
        self._dataset_type = dataset
        self._dataset = dataset(**kwargs)

    def _load(self) -> Any:
        return self._dataset.load()

    def _save(self, data: Any):
        self._dataset.save(data)

    def _describe(self) -> Dict[str, Any]:
        return self._dataset._describe()

    def _exists(self) -> bool:
        return self._dataset._exists()

    def _release(self):
        self._dataset._release()
