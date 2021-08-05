pandas-amazon-redshift
======================

**pandas-amazon-redshift** is a package to provide an interface between
the Amazon Redshift Data API and pandas.

This package allows you to pass data between Amazon Redshift and``pandas.DataFrame``
objects without managing database connections. 

In addtion, you can receive the following benefit from this package.

* When reading data from Redshift, this package cast data to appropriate data type
* Before writing data to Redshift, this package check the data. In case invalid data for specified
  data type, this package raises error before manipulating Redshift cluster like creating tables
  or inserting data, which means atomicity.

You can use this package for both private and commercial use while this project is
not officially supported by Amazon Web Services.

Now I am looking for contributors to enhance this package. If you are interested, feel free to pull
requests or reach out to me.
