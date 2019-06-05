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
-----------------------------
We want to store this DataFrame now as a dataset. Therefore, we first need
to connect to a storage location. ``kartothek`` can write to any location that
fulfills the `simplekv.KeyValueStore interface`_. We use `storefact`_ in this
example to construct such a store for the local filesystem.

.. ipython:: python

   from storefact import get_store_from_url
   from tempfile import TemporaryDirectory

   dataset_dir = TemporaryDirectory()
   store = get_store_from_url(f"hfs://{dataset_dir.name}")

Writing dataset to storage
--------------------------
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
      store, "a_unique_dataset_identifier", df, metadata_version=4
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
dataframes with different schemas as different into **one** dataset. This
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

As noted earlier, if no table name is provided by the user, ``kartothek``
assigns a default name to a table, it **does not** auto-generate unique
table names, so when passing in a list of dataframes without specifying
table names, a ``ValueError`` will be thrown if the schemas differ
across datasets.

For example, this throws a ``ValueError``:

.. ipython:: python
   :okexcept:

   dm = store_dataframes_as_dataset(
      store, "yet_another_unique_dataset_identifier", [df, df2], metadata_version=4
   )

But this runs fine:

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
      store, "yet_another_unique_dataset_identifier", [df, another_df], metadata_version=4
   )


Reading dataset from storage
----------------------------
After we have written the data, we want to read it back in again. For this we
use :func:`kartothek.io.eager.read_table`. This method
returns the whole dataset as a pandas DataFrame.


.. ipython:: python
   :okwarning:

   from kartothek.io.eager import read_table

   df = read_table("a_unique_dataset_identifier", store, table="table")
   df


Updating existing datasets
--------------------------

As noted at the beginning of this guide, ``kartothek`` is designed for large
datasets with contents that are too large to be held in a single machine. While
small, in-memory dataframes are good for getting started and learning the core
concepts, in a production setting a way to write data in batches is useful.
For this purpose, ``kartothek`` offers :func:`kartothek.io.eager.update_dataset_from_dataframes`
and :func:`kartothek.io.iter.update_dataset_from_dataframes__iter`. To see how to
update data in an existing dataset, lets reuse ``another_df`` from the example
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
of ``another_df`` appended to the contents of ``df``. In fact, the way
:func:`kartothek.io.eager.update_dataset_from_dataframes` works, a new table
(that is, a dataframe with a _different_ schema) **cannot** be added to an
existing dataset with an update.

Once users have written multiple dataframes with differing schemas to a dataset,
they would also need the ability to update the tables within with new data. The
following example shows how this can be acheived.

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

A subset of tables **cannot** be updated and running the following update
example instead throws a ``ValueError``:

.. ipython:: python
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
             "table2": another_df2
          }
       },
       store=store_factory,
       dataset_uuid="another_unique_dataset_identifier"
       )
   dm

.. _simplekv.KeyValueStore interface: https://simplekv.readthedocs.io/en/latest/#simplekv.KeyValueStore
.. _storefact: https://github.com/blue-yonder/storefact
