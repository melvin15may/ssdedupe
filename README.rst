============================
ssdedupe
============================

.. image:: https://img.shields.io/pypi/v/ssdedupe.svg
        :target: https://pypi.python.org/pypi/ssdedupe

.. image:: https://img.shields.io/travis/melvin15may/ssdedupe.svg?branch=master
        :target: https://travis-ci.org/melvin15may/ssdedupe

.. image:: https://codecov.io/gh/melvin15may/ssdedupe/branch/master/graph/badge.svg
	    :target: https://codecov.io/gh/melvin15may/ssdedupe

.. image:: https://readthedocs.org/projects/ssdedupe/badge/?version=latest
        :target: https://ssdedupe.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status

.. image:: https://pyup.io/repos/github/melvin15may/ssdedupe/shield.svg
     :target: https://pyup.io/account/repos/github/melvin15may/ssdedupe/
     :alt: Updates


**This is a fork from** `dssg/pgdedupe <http://github.com/dssg/pgdedupe>`_. **This will now be a separate repo for MS SQL Server implementation.** (See `PR#40 <https://github.com/dssg/pgdedupe/pull/40>`_)

**This packages is for working with Microsoft SQL Server. Please use pgdedupe for working with PostgreSQL**

A work-in-progress to provide a standard interface for deduplication of large
databases with custom pre-processing and post-processing steps.


* Free software: MIT license
* Documentation: https://ssdedupe.readthedocs.io.


Interface
---------

This provides a simple command-line program, ``ssdedupe``. Two configuration
files specify the deduplication parameters and database connection settings. To
run deduplication on a generated dataset, create a ``database.yml`` file that
specifies the following parameters::

	user:
	password:
	database:
	host:
	port:

You can now create a sample CSV file with::

	$ python generate_fake_dataset.py --csv people.csv
	creating people: 100%|█████████████████████| 9500/9500 [00:21<00:00, 445.38it/s]
	adding twins: 100%|█████████████████████████| 500/500 [00:00<00:00, 1854.72it/s]
	writing csv:  47%|███████████▋             | 4666/10000 [00:42<00:55, 96.28it/s]

Once complete, store this example dataset in a database with::

	$ python test/initialize_db.py --db database.yml --csv people.csv
	CREATE SCHEMA
	DROP TABLE
	CREATE TABLE
	COPY 197617
	ALTER TABLE
	ALTER TABLE
	UPDATE 197617

Now you can deduplicate this dataset. This will run dedupe as well as the
custom pre-processing and post-processing steps as defined in config.yml::

	$ ssdedupe --config config.yml --db database.yml


Custom pre- and post-processing
-------------------------------

In addition to running a database-level deduplication with ``dedupe``, this
script adds custom pre- and post-processing steps to improve the run-time and
results, making this a hybrid between fuzzy matching and record linkage.

* **Pre-processing:** Before running dedupe, this script does an exact-match
  deduplication. Some systems create many identical rows; this can make it
  challenging for dedupe to create an effective blocking strategy and generally
  makes the fuzzy matching much harder and time intensive.

* **Post-processing:** After running dedupe, this script does an optional
  exact-match merge across subsets of columns. For example, in some instances
  an exact match of just the last name and social security number are
  sufficient evidence that two clusters are indeed the same identity.


Further steps
-------------

This script was based upon and extended from the example in `dedupe-examples`_. It would be nice to use this common interface across all
database types, and potentially even allow reading from flat CSV files.

.. _dedupe-examples: https://github.com/datamade/dedupe-examples/tree/master/pgsql_big_dedupe_example
