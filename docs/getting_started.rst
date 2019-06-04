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
of the new data matches the schema of the existing data. If it doesn't, we will
raise an exception.

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


.. _simplekv.KeyValueStore interface: https://simplekv.readthedocs.io/en/latest/#simplekv.KeyValueStore
.. _storefact: https://github.com/blue-yonder/storefact
