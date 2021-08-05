.. pandas-amazon-redshift documentation master file, created by
   sphinx-quickstart on Wed Jul 28 08:31:44 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to pandas-amazon-redshift's documentation!
==================================================

The :mod:`pandas_amazon_redshift` module provides a wrapper for
Amazon Redshift analytics web service to simplify retrieving results
from Redshift tables using SQL. Result sets are parsed into a
:class:`pandas.DataFrame` with a shape and data types derived from
the source table. Additionally, DataFrames can be inserted into
new Redshift tables.

.. note::

   To use this module, you will need a valid Redshift cluster. Use the
   `Amazon Redshift free trial <https://aws.amazon.com/jp/redshift/free-trial/>`__ to
   try the service for free.

This module is appropriate for small (< 100 MB) data set to both read and write.
If where you are required to handle >=100 MB data, you should look to use S3.
When writing large data to Redshift, you should use ``COPY FROM s3`` statement.
When reading large data from Redshift and write result into file, you should use
``UNLOAD TO s3`` statement.
   
While Redshift uses standard SQL syntax, make sure Redshift is one of DWH
services, not a transactional database service.
Quick analytics on large data set is the best use case for Redshift.
Redshift is not appropriate for such OLTP workload where DMLs frequently run.
See detail in the `Amazon Redshift Documentation <https://docs.aws.amazon.com/redshift/index.html>`__.

Contents:

.. toctree::
   :maxdepth: 2

   reading.rst
   writing.rst
   core.rst
   types.rst
   errors.rst

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
