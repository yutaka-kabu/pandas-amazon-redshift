Reading Tables
==============

Use the :func:`pandas_redshift.read_redshift` function to run a query to
Redshift and download the results as a :class:`pandas.DataFrame` object.

.. literalinclude:: ../../samples/read_redshift_simple.py
   :language: python
   :dedent: 4
   :start-after: [START pandas_redshift_read_redshift_simple]
   :end-before: [END pandas_redshift_read_redshift_simple]

.. note::

    A cluster identifier and a database name is required to identify the
    cluster and database to access. In addition, A database user name is
    required, or you can designate Secret Arn instead for authentication
    to access to Redshift. 

.. _reading-dtypes:

Inferring the DataFrame's dtypes
================================

The :func:`~pandas_gbq.read_gbq` method infers the pandas dtype for each
column, based on the Redshift table schema.

=================== ====================================================================
Redshift Data Type  dtype
=================== ====================================================================
SMALLINT (nullable) Int64
SMALLINT NOT NULL   int16
INTEGER (nullable)  Int64
INTEGER NOT NULL    int32
BIGINT (nullable)   Int64
BIGINT NOT NULL     int64
DECIMAL             :class:`~decimal.Decimal`
REAL                float64
DOUBLE PRECISION    float64
BOOLEAN (nullable)  boolean
BOOLEAN NOT NULL    bool
CHAR                str
VARCHAR             str
DATE                :class:`~datetime.date`
TIMESTAMP           datetime64[ns]
TIMESTAMPTZ         :class:`~pandas.DatetimeTZDtype` with ``unit='ns'`` and ``tz='UTC'``
GEOMETRY            str
TIME                :class:`~datetime.time`
TIMETZ              :class:`~datetime.time`
SUPER               NA, int, float, str, list, or dict
=================== ====================================================================
