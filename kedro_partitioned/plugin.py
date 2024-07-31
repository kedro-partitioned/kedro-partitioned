"""Hook to enable MultiNode."""

from copy import deepcopy
from typing import Dict, Any
from kedro.pipeline import Pipeline
from kedro.io import DataCatalog
from kedro.framework.hooks import hook_impl
from kedro_datasets.json import JSONDataset
from kedro_partitioned.pipeline.multinode import _SlicerNode, _MultiNode
from upath import UPath
from kedro_datasets.partitions import PartitionedDataset
from kedro.framework.project import pipelines


class MultiNodeEnabler:
    """Performs required changes in kedro in order to enable MultiNodes.

    >>> from kedro.io import DataCatalog
    >>> from kedro_partitioned.io import PathSafePartitionedDataset
    >>> from kedro.pipeline import Pipeline, node
    >>> from kedro_partitioned.pipeline import multipipeline
    >>> pipe = multipipeline(Pipeline([
    ...     node(func=lambda x: x, name='node', inputs='a', outputs='b'),]),
    ...     'a', 'pipe', n_slices=2)
    >>> catalog = DataCatalog(datasets={
    ...     'a': PathSafePartitionedDataset(path='a', dataset='pandas.CSVDataset'),
    ...     'b': PathSafePartitionedDataset(path='b', dataset='pandas.CSVDataset')})
    >>> hook = MultiNodeEnabler()
    >>> hook.before_pipeline_run({}, pipe, catalog)

    >>> pprint(catalog._datasets)  # doctest: +ELLIPSIS
    {'a': <...PathSafePartitionedDataset ...>,
     'b': <...PathSafePartitionedDataset ...>,
     'b-slice-0': <...PathSafePartitionedDataset ...>,
     'b-slice-1': <...PathSafePartitionedDataset ...>,
     'b-slicer': <...JSONDataset ...>}

    >>> catalog._datasets['b-slicer']._filepath
    PurePosixPath('b/b-slicer.json')

    Azure Blob Storage:


    >>> credentials = {'account_name': 'test'}
    >>> catalog = DataCatalog(datasets={
    ...     'a': PathSafePartitionedDataset(path='http://a/a', dataset='pandas.CSVDataset',
    ...         credentials=credentials),
    ...     'b': PathSafePartitionedDataset(path='http://a/b', dataset='pandas.CSVDataset',
    ...         credentials=credentials)})
    >>> hook.before_pipeline_run({}, pipe, catalog)

    >>> catalog._datasets['b-slicer']._filepath
    PurePosixPath('a/b/b-slicer.json')

    >>> catalog._datasets['b-slicer']._protocol
    'http'
    """
    @hook_impl
    def after_context_created(self, context) -> None:
        self.pipe = pipelines["__default__"]
        
    @hook_impl
    def before_pipeline_run(
        self,
        run_params: Dict[str, Any],
        pipeline: Pipeline,
        catalog: DataCatalog,
    ):
        """Performs required changes in kedro in order to enable MultiNodes.

        Args:
            run_params (Dict[str, Any]): Dictionary of parameters to be fed.
            pipeline (Pipeline): Pipeline to be run.
            catalog (DataCatalog): Catalog of data sources.
        """
        for node in self.pipe.nodes:
            if isinstance(node, _MultiNode):
                for original, slice in zip(
                    node.original_partitioned_outputs, node.partitioned_outputs
                ):
                    partitioned = catalog._get_dataset(original)
                    assert isinstance(
                        partitioned, PartitionedDataset
                    ), "multinode cannot have non partitioned outputs"

                    #if not catalog.exists(slice):

                    cpy = deepcopy(partitioned)

                    setattr(cpy, 'slice_id', node._slice_id)

                    setattr(cpy, 'slice_count', node.slice_count)
                    print(cpy)
                    print(slice)
                    catalog.add(slice, deepcopy(partitioned))

                for input in node.original_partitioned_inputs:
                    partitioned = catalog._get_dataset(input)
                    assert isinstance(partitioned, PartitionedDataset), (
                        f'multinode received "{input}" as a '
                        f"`PartitionedDataset`, although it is a "
                        f"`{type(partitioned)}`"
                    )

            elif isinstance(node, _SlicerNode):
                partitioned = catalog._get_dataset(node.original_output)
                assert isinstance(partitioned, PartitionedDataset), (
                    f'multinode received "{node.original_output}" as a '
                    f"`PartitionedDataset`, although it is a "
                    f"`{type(partitioned)}`"
                )
                catalog.add(
                    node.json_output,
                    JSONDataset(
                        filepath=str(
                            UPath(partitioned._path) / f"{node.json_output}.json"
                        ),
                        credentials=partitioned._credentials,
                    ),
                )


multinode_enabler = MultiNodeEnabler()
