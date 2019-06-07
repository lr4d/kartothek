===============
Getting started
===============

``kartothek`` manages datasets that consist of files that contain tables.
When working with these tables as a Python user, we will use pandas DataFrames
as the user-facing type. We typically expect that the contents of a dataset are
large, often too large to be held in memory by a single machine but for demonstration
purposes, we will use a small DataFrame with a mixed set of types.

.. ipython:: python

    import numpy as np
    import pandas as pd

    df = pd.DataFrame(
        {
            "A": 1.,
            "B": pd.Timestamp("20130102"),
            "C": pd.Series(1, index=list(range(4)), dtype="float32"),
            "D": np.array([3] * 4, dtype="int32"),
            "E": pd.Categorical(["test", "train", "test", "train"]),
            "F": "foo",
        }
    )
    df

Defining the storage location
=============================
We want to store this DataFrame now as a dataset. Therefore, we first need
to connect to a storage location. ``kartothek`` can write to any location that
fulfills the `simplekv.KeyValueStore interface`_. We use `storefact`_ in this
example to construct such a store for the local filesystem.

.. ipython:: python
    :okexcept:

    from functools import partial
    from storefact import get_store_from_url
    from tempfile import TemporaryDirectory

    dataset_dir = TemporaryDirectory()

    store_factory = partial(get_store_from_url, f"hfs://{dataset_dir.name}")
    store = store_factory()


Writing dataset to storage
===========================
Now that we have our data and the storage location, we can persist the dataset.
For that we use in this guide :func:`kartothek.io.eager.store_dataframes_as_dataset`
to store a ``DataFrame`` we already have in memory in the local task.

.. ipython:: python
    :okexcept:
    :okwarning:

    from kartothek.io.eager import store_dataframes_as_dataset
    dm = store_dataframes_as_dataset(
        store,
        "a_unique_dataset_identifier",
        df,
        metadata_version=4
    )
    dm

.. admonition:: Storage backends

    The import path of this function already gives us a hint about the general
    structuring of the ``kartothek`` modules. In :mod:`kartothek.io` we have all
    the building blocks to build data pipelines that read and write from/to storages.
    The next module level (``eager``) describes the scheduling backend.

    The scheduling backends supported by kartothek are:

    - ``eager`` runs all execution immediately and on the local machine.
    - ``iter`` executes operations on the dataset on a per-partition basis.
      The standard format to read/store dataframes in ``iter`` is by providing
      a generator of dataframes.
    - ``dask`` is suitable for larger datasets. It can be used to work on datasets in
      parallel or even in a cluster by using ``dask.distributed`` as the backend.
      There are also ``dask.bag`` and ``dask.dataframe`` which support I/O operations
      for the respective `dask`_ objects.

After calling :func:`~kartothek.io.eager.store_dataframes_as_dataset`,
a :class:`kartothek.core.dataset.DatasetMetadata` object is returned. 
This class holds information about the structure and schema of the dataset.

For this guide, two attributes that are noteworthy are ``tables`` and ``partitions``:

- Each dataset has one or more ``tables``, where each table represents a particular subset of
  data, this data is stored as a collection of dataframes/files which have the same schema.
- Data is written to storage in batches (for ``eager``, there is only a single batch),
  in this sense a batch is termed a ``partition`` in ``kartothek``.
  Partitions are structurally identical to each other, thus, each partition of a dataset has the
  same number of dataframes (one for each table) as the rest of partitions.


For each table, ``kartothek`` also tracks the schema of the columns.
Unless specified explicitly on write, it is inferred from the passed data.
On writing additional data to a dataset, we will also check that the schema
of the new data matches the schema of the existing data.
A ``ValueError`` will be thrown if there is a mismatch in the schema. For example,
passing a list of dataframes with differing schemas and without table names to
:func:`kartothek.io.eager.store_dataframes_as_dataset`.

.. admonition:: Passing multiple partitions to a dataset during write/update

    To store multiple dataframes into a dataset (i.e. multiple `partitions`), it is possible
    to pass an iterator of dataframes, the exact format will depend on the I/O backend used.

    If passing an iterator of dataframes, and table names are not specified, ``kartothek``
    assumes these dataframes are different partitions with a single table.

As we have not explicitly defined the name of the table nor the name
of the created partition, ``kartothek`` has used the default table name
``table`` and generated a UUID for the partition name.

.. ipython:: python
    :okexcept:
    :okwarning:

    dm.tables
    dm.partitions


.. admonition:: A more complex example: multiple tables and partitions

    Sometimes it may be useful to write multiple dataframes with different schemas into
    a single dataset. This can be achieved by creating a dataset with multiple tables.

    In this example, we create a dataset with two partitions (represented by
    the dictionary objects inside the list).
    For each partition, there exist two tables: ``core-table`` and ``aux-table``.
    The schemas of the tables are identical across partitions.

    .. ipython:: python
       :okwarning:
       :okexcept:

       dfs = [
            {
                "data": {
                    "core-table": pd.DataFrame({"col1": ["x"]}),
                    "aux-table": pd.DataFrame({"f": [1.1]}),
                },
            },
            {
                "data": {
                    "core-table": pd.DataFrame({"col1": ["y"]}),
                    "aux-table": pd.DataFrame({"f": [1.2]}),
                },
            },
       ]

       store_dataframes_as_dataset(store, dataset_uuid="two-tables", dfs=dfs)

.. For example, this will not work:

.. .. ipython:: python
..     :okwarning:
..     :okexcept:

..     df2 = pd.DataFrame(
..         {
..             "G": "foo",
..             "H": pd.Categorical(["test", "train", "test", "train"]),
..             "I": np.array([3] * 4, dtype="int32"),
..             "J": pd.Series(1, index=list(range(4)), dtype="float32"),
..             "K": pd.Timestamp("20130102"),
..             "L": 1.,
..         }
..     )

..     store_dataframes_as_dataset(
..         store,
..         dataset_uuid="another_unique_dataset_identifier",
..         dfs = {
..             "table1": df,
..             "table2": df2
..         },
..     )

.. If dataframes (all with the same schema) are passed in 'anonymously'
.. as a list, they are essentially interpreted by ``kartothek`` as
.. different partitions of the `same` table.
    


Reading dataset from storage
=============================
After we have written the data, we may want to read it back in again. For this we can
use :func:`kartothek.io.eager.read_table`. This method returns the complete
table of the dataset as a pandas DataFrame (since there is only a single table in this
example, it returns the entire dataset).

.. ipython:: python
    :okwarning:
    :okexcept:

    from kartothek.io.eager import read_table

    df = read_table("a_unique_dataset_identifier", store, table="table")
    df

We could also read a dataframe iteratively, using 
:func:`kartothek.io.iter.read_dataset_as_dataframes__iterator`. This would return a generator
of dictionaries (one dictionary for each `partition`), where the keys of each dictionary
represent the `tables` of the dataset.

For example,

.. ipython:: python
    :okwarning:
    :okexcept:

    from kartothek.io.iter import read_dataset_as_dataframes__iterator

    for part_n, df_dict in enumerate(
            read_dataset_as_dataframes__iterator(dataset_uuid="two-tables", store=store_factory)
        ):
            print(f"Partition number: {part_n}")
            for table, df in df_dict.items():
                print(f"Table: {table}. Data: {df}")


Updating existing datasets
==========================

Once we have a dataset in storage, it would be useful to be able to update the data in them.
This is possible by adding new partitions using update functions that generally have the prefix
`update_dataset` in their names. For example, :func:`kartothek.io.eager.update_dataset_from_dataframes`
is the update function for the ``eager`` backend, whereas
:func:`kartothek.io.iter.update_dataset_from_dataframes__iter` is the update function for the ``iter`` one.

To see how to update data in an existing dataset, lets reuse ``another_df`` from the example
above and use the update functionality from ``eager`` to do so:

.. ipython:: python
    :okwarning:
    :okexcept:

    from kartothek.io.eager import update_dataset_from_dataframes

    another_df = pd.DataFrame(
        {
            "A": 1.,
            "B": pd.Timestamp("20130102"),
            "C": pd.Series(1, index=list(range(4)), dtype="float32"),
            "D": np.array([3] * 4, dtype="int32"),
            "E": pd.Categorical(["test", "train", "test", "train"]),
            "F": "foo",
        }
    )

    dm = update_dataset_from_dataframes(
        [another_df],
        store=store_factory,
        dataset_uuid="a_unique_dataset_identifier"
    )
    dm

Of interest now is ``dm.partitions`` - we can see that another partition has
been added. What this translates to in terms of files added is that another
``parquet`` file has been added to the store.

.. ipython:: python
    :okexcept:
    :okwarning:

    dm.partitions
    store.keys()

Also note that the ``store`` argument of :func:`kartothek.io.eager.update_dataset_from_dataframes`
requires a factory method.

Let's now see what happens when we read this data back:

.. ipython:: python
    :okexcept:
    :okwarning:

    df_again = read_table("a_unique_dataset_identifier", store, table="table")
    df_again

Since we updated the contents of ``another_df`` into the dataset with uuid
``a_unique_dataset_identifier`` and (again) didn't specify a table name, the
default table was updated and ``df_again`` now effectively contains the contents
of ``another_df`` appended to the contents of ``df``.

The way dataset updates works is that new partitions can be added for a dataset
as long as they have the same tables as the existing partitions. A `different`
table **cannot** introduced into an existing dataset with an update.

Once users have written multiple (named) tables to a dataset, they would also
need the ability to update these tables with new data. Updates require that all
tables of a dataset must be updated together and a subset of tables **cannot** be
individually updated.

Updating an existing dataset with new table data:

.. ipython:: python
    :okwarning:
    :okexcept:

    another_df2 = pd.DataFrame(
        {
            "G": "bar",
            "H": pd.Categorical(["test", "train", "test", "train"]),
            "I": np.array([6] * 4, dtype="int32"),
            "J": pd.Series(2, index=list(range(4)), dtype="float32"),
            "K": pd.Timestamp("20190604"),
            "L": 2.,
        }
    )
    another_df2

    dm = update_dataset_from_dataframes(
        {
            "data":
            {
                "table1": another_df,
                "table2": another_df2
            }
        },
        store=store_factory,
        dataset_uuid="another_unique_dataset_identifier"
    )
    dm


Trying to update a subset of tables throws a ``ValueError``:

.. ipython::

   @verbatim
   In [45]: update_dataset_from_dataframes(
      ....:        {
      ....:           "data":
      ....:           {
      ....:              "table2": another_df2
      ....:           }
      ....:        },
      ....:        store=store_factory,
      ....:        dataset_uuid="another_unique_dataset_identifier"
      ....:        )
      ....:
   ---------------------------------------------------------------------------
   ValueError: Input partitions for update have different tables than dataset:
   Input partition tables: {'table2'}
   Tables of existing dataset: ['table1', 'table2']




Garbage collection
==================
When ``kartothek`` is executing an operation, it makes sure to not
commit changes to the dataset until the operation has been succesfully completed. If a
write operation does not succeed for any reason, although there may be new files written
to storage, those files will not used by the dataset as they will not be referenced in
the ``kartothek`` metadata. Thus, when the user reads the dataset, no new data will
appear in the output.

Similarly, when deleting a partition, ``kartothek`` only removes the reference of that file
from the metadata.


These temporary files will remain in storage until a ``kartothek``  garbage collection
function is called on the dataset.
If a dataset is updated on a regular basis, it may be useful to run garbage collection
periodically to decrease unnecessary storage use.

An example of garbage collection is shown below. A file named ``trash.parquet`` is
created in storage but untracked by kartothek. When garbage collection is called, the
file is removed.

.. ipython:: python
   :okexcept:
   :okwarning:

   from kartothek.io.eager import garbage_collect_dataset

   # Put corrupt parquet file in storage for dataset "a_unique_dataset_identifier"
   store.put("a_unique_dataset_identifier/table/trash.parquet", b"trash")
   files_before = set(store.keys())

   garbage_collect_dataset(store=store_factory, dataset_uuid="a_unique_dataset_identifier")

   files_before.difference(store.keys())  # Show files removed


.. _simplekv.KeyValueStore interface: https://simplekv.readthedocs.io/en/latest/#simplekv.KeyValueStore
.. _storefact: https://github.com/blue-yonder/storefact
.. _dask: https://docs.dask.org/en/latest/
