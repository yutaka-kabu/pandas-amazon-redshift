Writing Tables
==============

Use the :func:`pandas_amazon_redshift.to_redshift` function to write a
:class:`pandas.DataFrame` object to a table in Redshift.

.. literalinclude:: ../../samples/to_redshift_simple.py
   :language: python
   :dedent: 4
   :start-after: [START pandas_amazon_redshift_to_redshift_simple]
   :end-before: [END pandas_amazon_redshift_to_redshift_simple]

The destination table will automatically be created.

Designating the Table Schema
============================

In the :func:`pandas_amazon_redshift.to_redshift` function,
``dtype`` is required to specify the table schema since
table schema inference has not been implemented.

For the argument ``dtype``, ``str``, :class:`pandas_amazon_redshift.RedshiftType`,
or ``dict`` is allowed. Basically ``dict`` is used for ``dtype``. Column
names are set for keys, and data type represented in ``str`` or
:class:`pandas_amazon_redshift.RedshiftType` are set for values in the ``dict``.
Where ``str`` or :class:`pandas_amazon_redshift.RedshiftType` is set as ``dtype``,
all columns will be parsed as the designatated value in ``dtype``.

You can use the following types for the argument ``dtype``.
Note that the ``str`` representation is case insensitive.

========================================== ====================================================
``str`` representation                     :class:`pandas_amazon_redshift.RedshiftType` representation
========================================== ====================================================
'SMALLINT', 'INT2'                         :class:`pandas_amazon_redshift.SmallInt`
'INTEGER', 'INT', 'INT4'                   :class:`pandas_amazon_redshift.Integer`
'BIGINT', 'INT8'                           :class:`pandas_amazon_redshift.BigInt`
'DECIMAL', 'NUMERIC'                       :class:`pandas_amazon_redshift.Numeric`
'REAL', 'FLOAT4'                           :class:`pandas_amazon_redshift.Real`
'DOUBLE PRECISION', 'FLOAT8', 'FLOAT'      :class:`pandas_amazon_redshift.DoublePrecision`
'BOOLEAN', 'BOOL'                          :class:`pandas_amazon_redshift.Boolean`
'CHAR', 'CHARACTER', 'NCHAR'               :class:`pandas_amazon_redshift.Char`
'BPCHAR'                                   :class:`pandas_amazon_redshift.BPChar`
'VARCHAR', 'CHARACTER VARYING', 'NVARCHAR' :class:`pandas_amazon_redshift.VarChar`
'TEXT'                                     :class:`pandas_amazon_redshift.Text`
'DATE'                                     :class:`pandas_amazon_redshift.Date`
'TIMESTAMP', 'TIMESTAMP WITHOUT TIME ZONE' :class:`pandas_amazon_redshift.TimeStamp`
'TIMESTAMPTZ', 'TIMESTAMP WITH TIME ZONE'  :class:`pandas_amazon_redshift.TimeStampTz`
'GEOMETRY'                                 :class:`pandas_amazon_redshift.Geometry`
'TIME', 'TIME WITHOUT TIME ZONE'           :class:`pandas_amazon_redshift.Time`
'TIMETZ', 'TIME WITH TIME ZONE'            :class:`pandas_amazon_redshift.TimeTz`
'SUPER'                                    :class:`pandas_amazon_redshift.Super`
========================================== ====================================================

Writing to an Existing Table
============================

Currently, this library does not support writing to an existing
table. Hence, you can set the argument ``if_exists`` only to
``'fail'``. 

Overview for Flow to Write a Table
==================================

The flow to write a table to Redshift cluster with
:func:`pandas_amazon_redshift.to_redshift` is summarized as follows:

1. Encode data to Redshift notations (type : ``str``) with checking data
2. Create table
3. Write data to the created table

In case data checking is failed mainly due to inappropriate data type,
``DataEncodeError`` is raised, the process is stopped, and the writer
neither create table nor write data.

:func:`pandas_amazon_redshift.to_redshift` writes data (step 3.) after confirming
finish of the statement to ``CREATE TABLE`` (step 2.). In writing data, multiple
statements for ``INSERT`` is executed in parallel. Note that you might encounter
unexpected results if you manipulate data or metadata for the created table
during writing data with :func:`pandas_amazon_redshift.to_redshift`, because each
statement in ``CREATE TABLE`` and ``INSERT`` sqls are executed in a different
transaction.

Data Checking
=============

Data Type Checking
------------------

Data type is checked from the viewpoint of 1/ unexpected string, which cannot
be associate with :class:`pandas_amazon_redshift.RedshiftType`, and 2/ invalid
arguments, e.g. >4,096 length for ``CHAR``.

Metadata Checking
-----------------

The metadata string to identify destination column, destination table, and destination
schema is checked. For the metadata string, 1/ characters which cannot be encoded to
``'utf-8'`` bytes, 2/ empty string, or 3/ NULL characters are not allowed.

Value Checking
--------------

The viewpoint to check data values depends on the data type.

Basically, any values which cannot be cased to specified data type
in ``dtype`` are not allowed.

Values to lead to overflow or underflow are not allowed for Numeric types.

For string types, characters which cannot be encoded to ``'utf-8'`` bytes
are not allowed.

For the ``SUPER`` type, only values which can be loaded to JSON are OK.

Troubleshooting Errors
======================

If an error occurs while writing data to Redshift, see
`Troubleshooting queries <https://docs.aws.amazon.com/redshift/latest/dg/queries-troubleshooting.html>`__
in Amazon Redshift Database Developer Guide.
