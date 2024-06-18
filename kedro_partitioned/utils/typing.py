"""Package for type annotations."""

from typing import Callable, Tuple, TypeVar, Union
from kedro_datasets.pandas import CSVDataset, ExcelDataset, ParquetDataset

T = TypeVar("T")
Args = Tuple[T]
IsFunction = Callable[[T], bool]
PandasDatasets = Union[CSVDataset, ExcelDataset, ParquetDataset]
