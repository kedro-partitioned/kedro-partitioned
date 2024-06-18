"""A Dataset that is partitioned into multiple Datasets."""

from pathlib import PurePosixPath
import posixpath
from kedro_datasets.partitions import PartitionedDataset


class PathSafePartitionedDataset(PartitionedDataset):
    """Partitioned Dataset, but handles mixed relative and absolute paths.

    For example, if the ffspec package you are using returns relative paths
    from a glob, but the path you specified is absolute, this dataset will be
    able to handle it.

    Example:
        >>> ds = PathSafePartitionedDataset(
        ...          path="http://abc.core/path/to",  # absolute
        ...          dataset="pandas.CSVDataset",)
        >>> ds._path_to_partition("path/to/partition1.csv")  # relative
        'partition1.csv'

        >>> ds = PartitionedDataset(
        ...          path="http://abc.core/path/to",  # absolute
        ...          dataset="pandas.CSVDataset",)
        >>> ds._path_to_partition("path/to/partition1.csv")  # relative
        'path/to/partition1.csv'
    """

    def _path_to_partition(self, path: str) -> str:
        """Takes only the relative subpath from the partitioned dataset path.

        Args:
            path (str): path to a partition

        Returns:
            str: relative subpath from the partitioned dataset path

        Example:
            >>> ds = PathSafePartitionedDataset(
            ...          path="http://abc.core/path/to",
            ...          dataset="pandas.CSVDataset",)
            >>> ds._path_to_partition("http://abc.core/path/to/partition1.csv")
            'partition1.csv'

            >>> ds = PathSafePartitionedDataset(
            ...          path="data/path",
            ...          dataset="pandas.CSVDataset",)
            >>> ds._path_to_partition("data/path/partition1.csv")
            'partition1.csv'

        Note:
            this dataset differs from the original one because it treats non
            absolute paths too. An example of non package that returns relative
            paths is the adlfs package. it returns the path relative to the
            container, while to declare the dataset, you'll have to pass the
            full uri to the folder. This makes Kedro's partitioned dataset to
            not rsplit(partition, path) correctly.
        """
        subpath = super()._path_to_partition(path)

        subpath_parts = PurePosixPath(path).parts
        path_parts = PurePosixPath(self._normalized_path).parts

        common_index = next(
            (i for i, part in enumerate(path_parts) if part == subpath_parts[0]), 0
        )
        suffix = str(PurePosixPath(*path_parts[common_index:])) + posixpath.sep
        return subpath.replace(suffix, "", 1)
