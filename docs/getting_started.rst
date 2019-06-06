===============
Getting started
===============

``kartothek`` manages datasets that consist of files that contain tables.
When working with these tables as a Python user, we will use pandas DataFrames
as the user-facing type. We typically expect that the dataset contents are
large, often too large to be held in a single machine but for demonstration
purposes, we use a small DataFrame with a mixed set of types.

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

    from storefact import get_store_from_url
    from tempfile import TemporaryDirectory

    dataset_dir = TemporaryDirectory()
    store = get_store_from_url(f"hfs://{dataset_dir.name}")

Writing datasets to storage
===========================
Now that we have our data and the storage location, we can persist the dataset.
For that we use in this guide :func:`kartothek.io.eager.store_dataframes_as_dataset`
to store a ``DataFrame`` we already have in memory in the local task.

.. admonition:: Storage backends

    The import path of this function already gives us a hint about the general
    structuring of the ``kartothek`` modules. In :mod:`kartothek.io` we have all
    the building blocks to build data pipelines that read and write from/to storages.
    Other top-level modules for example handle the serialization of DataFrames to
    ``bytes``.

    The next module level (``eager``) describes the scheduling backend.
    The scheduling backends supported by kartothek are:

    - ``eager`` runs all execution immediately and on the local machine.
    - ``iter`` executes operations on the dataset on a per-partition basis.
    - ``dask`` is suitable for larger datasets. It can be used to work on datasets in
      parallel or even in a cluster by using ``distributed`` as the backend for
      ``dask``.

.. ipython:: python
    :okwarning:

    from kartothek.io.eager import store_dataframes_as_dataset
    dm = store_dataframes_as_dataset(
        store,
        "a_unique_dataset_identifier",
        df,
        metadata_version=4
    )
    dm

After calling :func:`~kartothek.io.eager.store_dataframes_as_dataset`,
a :class:`kartothek.core.dataset.DatasetMetadata` object is returned. 
This class holds information about the structure and schema of the dataset.

For this guide, two attributes that are noteworthy are ``tables`` and ``partitions``:

- Each dataset has one or more tables, where each table represents a particular subset of
  data, this data is stored as a collection of dataframes/files which have the same schema.
- Data is written to storage in batches (for ``eager``, there is only a single batch),
  in this sense a batch is termed a ``partition`` in ``kartothek``.
  Partitions are structurally identical to each other, each partition of a dataset has the
  same number of dataframes (one for each table) as the rest of partitions.

.. admonition:: A more complex example: multiple tables

    In this example, we create a dataset with two partitions, named ``partition-1`` and 
    ``partition-2``.
    For each partition, there exist two tables: ``core-table`` and ``aux-table``.
    The schemas of the tables are identical across partitions.

    .. ipython:: python
        :okwarning:

        dfs = [
                {
                    "label": "partition-1",
                    "data": [
                        ("core-table", pd.DataFrame({"col1": ["x"]})),
                        ("aux-table", pd.DataFrame({"f": [1.1]})),
                    ],
                },
                {
                    "label": "partition-2",
                    "data": [
                        ("core-table", pd.DataFrame({"col1": ["y"]})),
                        ("aux-table", pd.DataFrame({"f": [1.2]})),
                    ],
                },
        ]

        store_dataframes_as_dataset(store, dataset_uuid="two-tables", dfs=dfs)

As we have not explicitly defined the name of the table nor the name
of the created partition, ``kartothek`` has used the default table name
``table`` and generated a UUID for the partition name.

.. ipython:: python

    dm.tables
    dm.partitions

For each table, ``kartothek`` also tracks the schema of the columns.
Unless specified explicitly on write, it is inferred from the passed data.
On writing additional data to a dataset, we will also check that the schema
of the new data matches the schema of the existing data. If it doesn't, we
will raise an exception.

Generally speaking, it would be useful for users to be able to write multiple
dataframes with different schemas into **one** dataset. This
can be done by explicitly declaring table names when writing:

.. ipython:: python

    df2 = pd.DataFrame(
        {
            "G": "foo",
            "H": pd.Categorical(["test", "train", "test", "train"]),
            "I": np.array([3] * 4, dtype="int32"),
            "J": pd.Series(1, index=list(range(4)), dtype="float32"),
            "K": pd.Timestamp("20130102"),
            "L": 1.,
        }
    )
    df2

    dm = store_dataframes_as_dataset(
        store,
        "another_unique_dataset_identifier",
        {
            "table1": df,
            "table2": df2
        },
        metadata_version=4
    )
    dm

If dataframes (all with the same schema) are passed in 'anonymously'
as a list, they are essentially interpreted by ``kartothek`` as
different partitions of the `same` table.

As noted earlier, if no table name is provided by the user, ``kartothek``
assigns a default name to a table, it **does not** auto-generate unique
table names. So when passing in a list of dataframes with differing schemas
and without specifying table names, a ``ValueError`` will be thrown.

For example, this will not work:

.. ipython:: python

    try:
        dm = store_dataframes_as_dataset(
            store,
            "yet_another_unique_dataset_identifier",
            [df, df2],
            metadata_version=4
        )
    except ValueError as ve:
        print("{}".format(ve.args[0]))

But this runs fine, because both dataframes passed in have identical schemas:

.. ipython:: python

    another_df = pd.DataFrame(
        {
            "A": 2.,
            "B": pd.Timestamp("20190604"),
            "C": pd.Series(2, index=list(range(4)), dtype="float32"),
            "D": np.array([6] * 4, dtype="int32"),
            "E": pd.Categorical(["test", "train", "test", "train"]),
            "F": "bar",
        }
    )
    another_df

    dm = store_dataframes_as_dataset(
        store,
        "yet_another_unique_dataset_identifier",
        [df, another_df],
        metadata_version=4
    )


Reading datasets from storage
=============================
After we have written the data, we want to read it back in again. For this we
use :func:`kartothek.io.eager.read_table`. This method
returns the whole dataset as a pandas DataFrame.


.. ipython:: python
    :okwarning:

    from kartothek.io.eager import read_table

    df = read_table("a_unique_dataset_identifier", store, table="table")
    df


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

    from kartothek.io.eager import update_dataset_from_dataframes
    from functools import partial

    store_factory = partial(get_store_from_url, f"hfs://{dataset_dir.name}")

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

    dm.partitions
    store.keys()

Also note that the ``store`` argument of :func:`kartothek.io.eager.update_dataset_from_dataframes`
requires a factory method.

Let's now see what happens when we read this data back:

.. ipython:: python

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

    df_again = read_table("another_unique_dataset_identifier", store, table="table1")
    df_again

    df2_again = read_table("another_unique_dataset_identifier", store, table="table2")
    df2_again

Trying to update a subset of tables throws a ``ValueError``:

.. ipython:: python

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

    try:
        dm = update_dataset_from_dataframes(
            {
                "data":
                {
                    "table2": another_df2
                }
            },
            store=store_factory,
            dataset_uuid="another_unique_dataset_identifier"
        )
        dm
    except ValueError as ve:
        print("{}".format(ve.args[0]))

Partitioning and secondary indices
==================================

``kartothek`` is designed primarily for storing large datasets consistently and
accessing them efficiently. To achieve this, it provides two useful functionalities:
partitioning and secondary indices.

Partitioning
------------

As we have already seen, updating a dataset in ``kartothek`` amounts to adding new
partitions, which in the underlying key-value store translates to writing new files
to the storage layer.

From the perspective of efficient access, it would be helpful if accessing a subset
of written data doesn't require reading through an entire dataset to be able to identify
and access the required subset. This is where partitioning by table columns helps.

Specifically, ``kartothek`` allows users to (physically) partition their data by the
values of table columns such that all the rows with the same value of the column all get
written to the same partition. To do this, we use the ``partition_on`` keyword argument:

.. ipython:: python

    dm = store_dataframes_as_dataset(
        store,
        "partitioned_dataset",
        df,
        partition_on = 'E',
        metadata_version=4
    )
    dm

Of interest here is ``dm.partitions``:

.. ipython:: python

    dm.partitions

    store.keys()

Partitioning can even be performed on multiple columns; in this case, columns needs to
be specified as a list:

.. ipython:: python

    dm = store_dataframes_as_dataset(
        store,
        "another_partitioned_dataset",
        [df, another_df],
        partition_on = ['E', 'F'],
        metadata_version=4
    )
    dm

    dm.partitions

Generally speaking, partitions are stored as
``<p_column_1_name>=<p_column_1_value>/.../<p_column_N_name>=<p_column_N_value>/<partition_label>``

For datasets consisting of multiple (therefore, named) tables, partitioning on
columns only works if the column exists in both tables and is of the same data type.

So, for example, (weirdly enough) this will work:

.. ipython:: python

    df3 = pd.DataFrame(
        {
            "G": "foo",
            "E": pd.Categorical(["test2", "train2", "test2", "train2"]),
            "I": np.array([3] * 4, dtype="int32"),
            "J": pd.Series(1, index=list(range(4)), dtype="float32"),
            "K": pd.Timestamp("20130102"),
            "L": 1.,
        }
    )
    df3

    dm = store_dataframes_as_dataset(
        store,
        "multiple_partitioned_tables",
        {
            "table1": df,
            "table2": df3
        },
        partition_on='E',
        metadata_version=4
    )
    dm

    dm.partitions

But the following two examples throw a ``ValueError``.

Example of error when the partition columns don't exist in all tables:

.. ipython:: python

    try:
        dm = store_dataframes_as_dataset(
            store,
            "erroneously_partitioned_dataset",
            {
                "table1": df,
                "table2": df2
            },
            partition_on = ['E', 'H'],
            metadata_version=4
        )
    except ValueError as ve:
        print("{}".format(ve.args[0]))

Example of error when the partition column exists in both tables but has
different types:

.. ipython:: python

    df4 = pd.DataFrame(
        {
            "G": "foo",
            "E": pd.Categorical([True, False, True, False]),
            "I": np.array([3] * 4, dtype="int32"),
            "J": pd.Series(1, index=list(range(4)), dtype="float32"),
            "K": pd.Timestamp("20130102"),
            "L": 1.,
        }
    )
    df4

    try:
        dm = store_dataframes_as_dataset(
            store,
            "another_erroneously_partitioned_dataset",
            {
                "table1": df,
                "table2": df4
            },
            partition_on='E',
            metadata_version=4
        )
    except ValueError as ve:
        print("{}".format(ve.args[0]))


Garbage collection
------------------
When ``kartothek`` is executing an operation, it makes sure to not
commit changes to the dataset until the operation has been succesfully completed. If a
write operation does not succeed for any reason, although there may be new files written
to storage, those files will not used by the dataset as they will not be referenced in
the ``kartothek`` metadata. Thus, when the user reads the dataset, no new data will
appear in the output.

Similarly, when deleting a partition, ``kartothek`` only removes the reference of that file
from the metadata.


These temporary files will remain in storage until garbage collection is called in
``kartothek``.

If a dataset is updated on a regular basis, it may be useful to run garbage collection
periodically to decrease unnecessary storage use.

An example of garbage collection is shown below. A file named ``trash.parquet`` is
created in storage but untracked by kartothek. When garbage collection is called, the
file is removed.

.. ipython:: python

   from kartothek.io.eager import garbage_collect_dataset

   # Put corrupt parquet file in storage
   store.put("a_unique_dataset_identifier/table/trash.parquet", b"trash")
   files_before = set(store.keys())

   garbage_collect_dataset(store=store_factory, dataset_uuid="a_unique_dataset_identifier")

   files_before.difference(store.keys())  # Show files removed


.. _simplekv.KeyValueStore interface: https://simplekv.readthedocs.io/en/latest/#simplekv.KeyValueStore
.. _storefact: https://github.com/blue-yonder/storefact
