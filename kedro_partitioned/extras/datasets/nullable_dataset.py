"""A Dataset that doesn't error when no data is found or received."""

from pathlib import PurePosixPath
from typing import Any, Type
from kedro_partitioned.extras.datasets.wrapper_dataset import WrapperDataset
from kedro.io import AbstractDataset
from kedro_partitioned.utils.other import FlagType


class NullType(FlagType):
    """Null flag, like None."""

    pass


"""
This object was created in order to avoid `Kedro` errors when
passing or returning `None` to/from a node
"""
Null = NullType()


def isnull(x: Any) -> bool:
    """Checks if an object is `Null`.

    Args:
        x (Any)

    Returns:
        bool

    Example:
        >>> isnull(3)
        False
        >>> isnull(Null)
        True
    """
    return x is Null


def isnotnull(x: Any) -> bool:
    """Checks if an object is not `Null`.

    Args:
        x (Any)

    Returns:
        bool

    Example:
        >>> isnotnull(3)
        True
        >>> isnotnull(Null)
        False
    """
    return not isnull(x)


class NullableDataset(WrapperDataset):
    """A Dataset that doesn't error when received or loaded Null/no data.

    Wraps a Dataset and returns Null if a load error occur and doesn't
    throw error if a Null object is provided into save

    Example:
        >>> from kedro_datasets.pandas import CSVDataset
        >>> csv = CSVDataset(filepath='__example_folder')
        >>> csv.save(Null) # doctest: +ELLIPSIS
        Traceback (most recent call last):
        ...
        kedro.io.core.DatasetError: ...

        >>> csv.load() # doctest: +ELLIPSIS
        Traceback (most recent call last):
        ...
        kedro.io.core.DatasetError: ...

        >>> null = NullableDataset(dataset=CSVDataset, verbose=False,
        ...                        filepath='__example_folder')
        >>> null._filepath
        PurePosixPath('__example_folder')
        >>> null.save(Null)

        >>> print(null.load())
        Null
    """

    def __init__(
        self, dataset: Type[AbstractDataset], verbose: bool = True, **kwargs: Any
    ):
        """Initializes a new instance of `NullableDataset`.

        Args:
            dataset (Type[AbstractDataset]): A Dataset class to wrap.
            verbose (bool, optional): Whether to log warnings.
                Defaults to True.
        """
        self._verbose = verbose
        super().__init__(dataset, **kwargs)

    @property
    def _filepath(self) -> PurePosixPath:
        if hasattr(self._dataset, "_filepath"):
            return self._dataset._filepath
        else:
            raise KeyError(f'"_filepath" property doesn\'t exist in {self._dataset}')

    def save(self, data: Any):
        """Saves data to the underlying Dataset.

        Args:
            data (Any): Data to save.
        """
        if data is Null:
            if self._verbose:
                self._logger.warning(
                    f'Received `Null` while saving into "{self._filepath}"'
                )
        else:
            super().save(data)

    def load(self) -> Any:
        """Loads data from the underlying Dataset.

        Returns:
            Any: Data loaded from the underlying Dataset.
        """
        try:
            return super().load()
        except Exception:
            if self._verbose:
                self._logger.warning(f'Could not load Dataset from "{self._filepath}"')
            return Null
