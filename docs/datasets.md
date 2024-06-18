# Datasets

## Concatenated Dataset

```{eval-rst}
.. autoclass::
   kedro_partitioned.extras.datasets.concatenated_dataset.ConcatenatedDataset
```

## Pandas Concatenated Dataset

Pandas concatenated dataset is a sugar for the `PartitionedDataset` that concatenates all dataframe partitions into a single dataframe.

For example, let's say you have a folder structure like this:

```md
clients/
├── brazil.csv
├── canada.csv
└── united_states.csv
```

And you wan't to load all the files as a single dataset. In this case, you could do something like this:

```yaml
clients:
  type: kedro_partitioned.dataset.PandasConcatenatedDataset
  path: clients
  dataset:
    type: pandas.CSVDataset
```

Then, the clients dataset will be all the concatenated dataframes from the `clients/*.csv` files.

```{eval-rst}
.. autoclass::
   kedro_partitioned.extras.datasets.concatenated_dataset.PandasConcatenatedDataset
```

## Path Safe Partitioned Dataset

```{eval-rst}
.. autoclass::
   kedro_partitioned.io.PathSafePartitionedDataset
```

```{note}
it is recommended to use PathSafePartitionedDataset instead of PartitionedDataset, for every step parallelism scenario. This is important because handling path safely is mandatory for the multinode partitioned dataset zip feature to work properly.
```
