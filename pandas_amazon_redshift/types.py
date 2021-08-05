from __future__ import annotations

import math
import re
from decimal import Decimal, ROUND_HALF_UP
from dateutil.parser._parser import ParserError
from typing import Dict, Hashable, Union
import json
import numpy
import pandas
from pandas import Series

from .utils import to_utf8_bytes
from .errors import InvalidRedshiftType

Dtype = Union[str, "RedshiftType"]
DtypeArg = Union[Dtype, Dict[Hashable, Dtype]]

_TYPE_REGEX = re.compile(r"^([a-zA-Z0-9 ]*)(\(([0-9, ]*?)\))?$")


def get_redshift_type(type_str):
    m = _TYPE_REGEX.match(type_str)
    if not m:
        raise InvalidRedshiftType(
            "Redshift type not found for '{}'".format(type_str)
        )

    type_name = m.group(1)
    type_args = m.group(3)

    type_name = type_name.upper().strip()

    type_dict = {
        "SMALLINT": SmallInt,
        "INT2": SmallInt,
        "INTEGER": Integer,
        "INT": Integer,
        "INT4": Integer,
        "BIGINT": BigInt,
        "INT8": BigInt,
        "DECIMAL": Numeric,
        "NUMERIC": Numeric,
        "REAL": Real,
        "FLOAT4": Real,
        "DOUBLE PRECISION": DoublePrecision,
        "FLOAT8": DoublePrecision,
        "FLOAT": DoublePrecision,
        "BOOLEAN": Boolean,
        "BOOL": Boolean,
        "CHAR": Char,
        "CHARACTER": Char,
        "NCHAR": Char,
        "BPCHAR": BPChar,
        "VARCHAR": VarChar,
        "CHARACTER VARYING": VarChar,
        "NVARCHAR": VarChar,
        "TEXT": Text,
        "DATE": Date,
        "TIMESTAMP": TimeStamp,
        "TIMESTAMP WITHOUT TIME ZONE": TimeStamp,
        "TIMESTAMPTZ": TimeStampTz,
        "TIMESTAMP WITH TIME ZONE": TimeStampTz,
        "TIME": Time,
        "TIME WITHOUT TIME ZONE": Time,
        "TIMETZ": TimeTz,
        "TIME WITH TIME ZONE": TimeTz,
        "GEOMETRY": Geometry,
        "SUPER": Super,
    }

    if type_name not in type_dict:
        raise InvalidRedshiftType(
            "Redshift type not found for '{}'".format(type_str)
        )
    redshift_type = type_dict[type_name]
    if type_args:
        type_args = [int(elm.strip()) for elm in type_args.split(",")]
    else:
        type_args = []

    return redshift_type(*type_args)


class RedshiftType(object):
    """An abstracttype for Redshift types.

    Each type has encoder and decoder.

    Methods
    -------
    encode :
        Encode objects stored in a column for pandas.DataFrame
        to ``str``-typed notations for Redshift DMLs.
    decode :
        Map raw values from Redshift Data API to the proper
        type, for ease of dealing.

    """

    _ESCAPES = [
        ("\\", "\\\\"),
        ("'", "\\'"),
        ("\n", "\\n"),
        ("\t", "\\t"),
        ("\b", "\\b"),
        ("\f", "\\f"),
    ]

    def _check(self, text, ubytes):
        pass

    def _encode_text(self, text):
        if pandas.isnull(text) or pandas.isna(text):
            return "NULL"

        ubytes = to_utf8_bytes(str(text))

        encoded_text = ubytes.decode("utf-8")

        self._check(encoded_text, ubytes)

        encoded_text = "\n".join(encoded_text.splitlines())

        for old, new in self._ESCAPES:
            encoded_text = encoded_text.replace(old, new)

        return "'{}'".format(encoded_text)

    def encode(self, col: Series) -> Series:
        """Encode objects stored in a column for pandas.DataFrame to
        ``str``-typed Redshift notations, which are used in DMLs.

        First, values are casted to string. Next, character encoding is
        changed to ``utf-8``, which Redshift supports as a multibyte
        character set. Next, strings are checked in terms of length or
        multibyte characters to avoid errors when running ``INSERT``
        statements. Then, escapes are replaced. Finally, the string is quoted.

        Parameters
        ----------
        col : pandas.Series
            The column storing original objects in pandas.DataFrame.

        Returns
        -------
        encoded_col : pandas.Series
            Column storing Redshift notations.

        """

        encoded_col = col.fillna(numpy.nan)
        encoded_col = encoded_col.map(self._encode_text)

        return encoded_col

    def decode(self, col: Series) -> Series:
        """Decode response from Redshift data api to Python ojects. See
        comments on each Redshift type class to confirm what type or class
        is used.

        Parameters
        ----------
        col :
            Column storing raw values in response from Redshift Data API.

        Returns
        -------
        col :
            Column storing Python objects.

        """

        return col

    def __str__(self):
        return self.__redshift_name__


class DoublePrecision(RedshiftType):
    """A type for Redshift ``DOUBLE PRECISION`` type.

    This type is decoded to numpy ``float64`` type.

    The encoder for this type accepts any types which are able to
    casted to numpy ``float64`` type.

    Methods
    -------
    encode :
        Encode objects stored in a column for pandas.DataFrame
        to ``str``-typed notations for Redshift DMLs.
    decode :
        Map raw values from Redshift Data API to the proper
        numpy float type, for ease of dealing.

    """

    __np_type__ = "float64"
    __redshift_name__ = "DOUBLE PRECISION"
    __min_abs__ = 2.22507385850721e-308
    __max_abs__ = 1.79769313486231e308
    __to_be_checked__ = True

    def _check_range(self, val):
        if pandas.isna(val) or val == 0.0:
            return val
        val_abs = abs(val)
        if val_abs < self.__min_abs__ or self.__max_abs__ < val_abs:
            raise TypeError(
                "'{}' is out of range for type '{}'".format(val, str(self))
            )
        return val

    def encode(self, col: Series) -> Series:
        """Encode objects stored in a column for pandas.DataFrame
        to ``str``-typed notations for Redshift DMLs.

        First, values are casted to numpy float type. Next, value
        range are checked to avoid overflow or underflow errors
        when running ``INSERT`` statements. Finally, the numeric
        types are casted to str.

        Parameters
        ----------
        col : pandas.Series
            The column storing original objects in pandas.DataFrame.

        Returns
        -------
        encoded_col : pandas.Series
            Column storing Redshift notations.

        """

        encoded_col = col.astype(self.__np_type__)

        encoded_col = encoded_col.fillna(numpy.nan)
        if self.__to_be_checked__:
            encoded_col.map(self._check_range)
        encoded_col = encoded_col.replace([numpy.nan], ["NULL"])
        encoded_col = encoded_col.astype(str)

        return encoded_col

    def decode(self, col: Series) -> Series:
        """Raw values in response from Redshift data api are represented
        in str or float types. This decoder will map these raw values to
        the proper numpy float type, for ease of dealing.

        Parameters
        ----------
        col :
            Column storing raw values in response from Redshift Data API.

        Returns
        -------
        col :
            Column storing numpy float values.

        """

        return col.astype(self.__np_type__)


class Real(DoublePrecision):
    """A type for Redshift ``REAL`` type.

    This type is decoded to numpy ``float64`` type since deciaml
    inaccuracy is observed in case of using numpy ``float32``.

    The encoder for this type accepts any values which are able to
    casted to numpy ``float64`` type and do not cause overflow or
    underflow for Redshift ``REAL`` type.

    Methods
    -------
    encode :
        Encode objects stored in a column for pandas.DataFrame
        to ``str``-typed notations for Redshift DMLs.
    decode :
        Map raw values from Redshift Data API to the proper
        numpy float type, for ease of dealing.

    """

    __redshift_name__ = "REAL"
    __min_abs__ = 1.1755e-38
    __max_abs__ = 3.40282e38


class Numeric(DoublePrecision):
    """A type for Redshift ``DECIMAL`` type.

    In this library, the alias ``NUMERIC`` is used instead to avoid
    conflict with Python ``decimal.Decimal`` type.

    There are not any fixed point types in numpy. This made us
    develop the decoder to cast values from Redshift Data API to
    Python ``decimal.Decimal``. Hense, the output for the decoder
    looks ``object``-type Series.

    The encoder for this type accepts any values which are able to
    casted to numpy ``float128`` type and do not cause overflow
    for the decimal with the specific precision and scale.

    Methods
    -------
    encode :
        Encode objects stored in a column for pandas.DataFrame
        to ``str``-typed notations for Redshift DMLs.
    decode :
        Map raw values from Redshift Data API to the proper
        ``decimal.Decimal`` type, for ease of dealing.

    """

    __np_type__ = "float128"
    __redshift_name__ = "NUMERIC"

    def __init__(self, precision: int = 18, scale: int = 0):
        """Construct the Redshift ``NUMERIC`` type.

        Parameters
        ----------
        precision :
            the numeric precision for use in DDL ``CREATE TABLE``.

        :param scale: the numeric scale for use in DDL ``CREATE TABLE``.

        """

        if precision != 18 or scale != 0:
            self.__redshift_name__ = "NUMERIC({},{})".format(precision, scale)
        self.__max_abs__ = Decimal(str(math.pow(10.0, precision - scale)))

        self.__exp_to_quantize__ = Decimal(
            "1.{}".format("".join(["0" for i in range(scale)]))
        )

    def _encode_numeric(self, val):
        if pandas.isna(val):
            return "NULL"

        decimal_val = Decimal(str(val)).quantize(
            self.__exp_to_quantize__, rounding=ROUND_HALF_UP
        )

        decimal_val_abs = abs(decimal_val)
        if self.__max_abs__ <= decimal_val_abs:
            raise TypeError(
                "'{}' is out of range for type '{}'".format(
                    decimal_val, str(self)
                )
            )
        return str(decimal_val)

    def encode(self, col: Series) -> Series:
        """Encode objects stored in a column for pandas.DataFrame
        to ``str``-typed notations for Redshift DMLs.

        First, values are casted to numpy.float128 type to avoid
        numeric inaccuracy. Next, numpy.float128 values are converted to
        decimal.Decimal values which are accurate under the conditions of
        ``precision`` and ``scale.`` Then, after checking min/max to avoid
        overflow error, values are casted to str.

        Parameters
        ----------
        col : pandas.Series
            The column storing original objects in pandas.DataFrame.

        Returns
        -------
        encoded_col : pandas.Series
            Column storing Redshift notations.
        """

        encoded_col = col.astype(self.__np_type__)

        encoded_col = encoded_col.fillna(numpy.nan)
        encoded_col = encoded_col.map(self._encode_numeric)
        return encoded_col

    def decode(self, col: Series) -> Series:
        """Raw values in response from Redshift data api are represented
        in str type. This decoder will cast these raw values represented
        in string to ``decimal.Decimal`` objects. To store
        ``decimal.Decimal`` objects, the ``dtype`` for the returned
        pandas.Series looks ``object``.

        Parameters
        ----------
        col :
            Column storing raw values in response from Redshift Data API.

        Returns
        -------
        col :
            Column storing ``decimal.Decimal`` values.

        """

        def _to_decimal(val):
            if pandas.isna(val):
                return numpy.nan

            return Decimal(val)

        return col.map(_to_decimal)


class Integer(DoublePrecision):
    """A type for Redshift ``INTEGER`` type.

    This type values are decoded to numpy ``int32`` in case NULL is
    not included, and otherwise decoded to pandas ``Int64``, which
    is the nullable integer type.

    The encoder for this type accepts any values which are able to
    casted to numpy ``Int64`` type and do not cause overflow
    for Redshift ``INTEGER`` type.

    Methods
    -------
    encode :
        Encode objects stored in a column for pandas.DataFrame
        to ``str``-typed notations for Redshift DMLs.
    decode :
        Map raw values from Redshift Data API to the proper
        numpy int type, for ease of dealing.

    """

    __np_type__ = "Int64"
    __np_type_not_null__ = "int32"
    __redshift_name__ = "INTEGER"
    __min__ = -2147483648
    __max__ = 2147483647

    def _check_range(self, val):
        if pandas.isna(val):
            return numpy.nan
        if val < self.__min__ or self.__max__ < val:
            raise TypeError(
                "'{}' is out of range for type '{}'".format(val, str(self))
            )
        return val

    def decode(self, col: Series) -> Series:
        """Raw values in response from Redshift data api are represented
        in str or int type. This decoder will cast these raw values to
        numpy int objects.

        If NULL is included, this decoder will use the nullable integer
        type ``Int64``. Otherwise, numpy integer types, which are ``int16``,
        ``int32``, or ``int64``, are used.

        Parameters
        ----------
        col :
            Column storing raw values in response from Redshift Data API.

        Returns
        -------
        col :
            Column storing numpy int values.

        """

        if len(col) != col.count():
            return col.astype(self.__np_type__)
        return col.astype(self.__np_type_not_null__)


class SmallInt(Integer):
    """A type for Redshift ``SMALLINT`` type.

    This type values are decoded to numpy ``int16`` in case NULL is
    not included, and otherwise decoded to pandas ``Int64``, which
    is the nullable integer type.

    The encoder for this type accepts any values which are able to
    casted to numpy ``Int64`` type and do not cause overflow
    for Redshift ``SMALLINT`` type.

    Methods
    -------
    encode :
        Encode objects stored in a column for pandas.DataFrame
        to ``str``-typed notations for Redshift DMLs.
    decode :
        Map raw values from Redshift Data API to the proper
        numpy int type, for ease of dealing.

    """

    __np_type_not_null__ = "int16"
    __redshift_name__ = "SMALLINT"
    __min__ = -32768
    __max__ = 32767


class BigInt(Integer):
    """A type for Redshift ``BIGINT`` type.

    This type values are decoded to numpy ``int64`` in case NULL is
    not included, and otherwise decoded to pandas ``Int64``, which
    is the nullable integer type.

    The encoder for this type accepts any values which are able to
    casted to numpy ``Int64`` type.

    Methods
    -------
    encode :
        Encode objects stored in a column for pandas.DataFrame
        to ``str``-typed notations for Redshift DMLs.
    decode :
        Map raw values from Redshift Data API to the proper
        numpy int type, for ease of dealing.

    """

    __np_type_not_null__ = "int64"
    __redshift_name__ = "BIGINT"
    __to_be_checked__ = False


class Boolean(Integer):
    """A type for Redshift ``BOOLEAN`` type.

    This type values are decoded to numpy ``bool`` in case NULL is
    not included, and otherwise decoded to pandas ``boolean``, which
    is the nullable integer type.

    The encoder for this type accepts any values which are able to
    casted to numpy ``boolean`` type.

    Methods
    -------
    encode :
        Encode objects stored in a column for pandas.DataFrame
        to ``str``-typed notations for Redshift DMLs.
    decode :
        Map raw values from Redshift Data API to the proper
        numpy bool type, for ease of dealing.

    """

    __np_type__ = "boolean"
    __np_type_not_null__ = "bool"
    __redshift_name__ = "BOOLEAN"
    __to_be_checked__ = False

    def decode(self, col: Series) -> Series:
        """Raw values in response from Redshift data api are represented
        in str type. This decoder will cast these raw values to numpy
        boolean objects.

        If NULL is included, this decoder will use the nullable boolean
        type ``boolean``. Otherwise, numpy boolean type ``bool`` is used.

        Parameters
        ----------
        col :
            Column storing raw values in response from Redshift Data API.

        Returns
        -------
        col :
            Column storing numpy bool values.

        """

        decoded_col = col.map(
            lambda x: numpy.nan if pandas.isna(x) else (x == "true")
        )
        return super().decode(decoded_col)


class Char(RedshiftType):
    """A type for Redshift ``CHAR`` type.

    This type values are decoded to Python ``str``.

    The encoder for this type accepts strings, but it rejects multibyte
    characters and too long strings.

    Methods
    -------
    encode :
        Encode objects stored in a column for pandas.DataFrame
        to ``str``-typed notations for Redshift DMLs.
    decode :
        Map raw values from Redshift Data API to the Python
        ``str`` type, for ease of dealing.

    """

    __multibyte_is_allowed__ = False
    __redshift_name__ = "CHAR"
    __default_length__ = 1
    __max_length__ = 4096

    def __init__(self, length: int = 0):
        """Construct the Redshift ``CHAR``type.

        Parameters
        ----------
        length :
            Length limitation.

        """

        self.length = self.__default_length__ if length == 0 else length
        if self.length != self.__default_length__:
            if self.length > self.__max_length__:
                raise InvalidRedshiftType(
                    "The length '{}' is too long for '{}'".format(
                        self.length, self.__redshift_name__
                    )
                )
            self.__redshift_name__ = "{}({})".format(
                self.__redshift_name__, length
            )

    def _check(self, text, ubytes):
        if (not self.__multibyte_is_allowed__) and len(text) != len(ubytes):
            raise TypeError("multibyte characters must not be included")
        if len(ubytes) > self.length:
            raise TypeError(
                "'{}' exceeds length ({})".format(text, self.length)
            )


class BPChar(Char):
    """A type for Redshift ``BPCHAR`` type. This type is alias for
    ``CHAR`` type, but the specification about length is different:
    the length is fixed as 256.

    Methods
    -------
    encode :
        Encode objects stored in a column for pandas.DataFrame
        to ``str``-typed notations for Redshift DMLs.
    decode :
        Map raw values from Redshift Data API to the Python
        ``str`` type, for ease of dealing.

    """

    __redshift_name__ = "BPCHAR"
    __default_length__ = 256

    def __init__(self):
        """Construct the Redshift ``BPCHAR`` type."""

        self.length = self.__default_length__


class VarChar(Char):
    """A type for Redshift ``VARCHAR`` type.

    This type values are decoded to Python ``str``.

    The encoder for this type accepts strings. Unlike ``CHAR`` type,
    this type accepts multibyte characters.

    Methods
    -------
    encode :
        Encode objects stored in a column for pandas.DataFrame
        to ``str``-typed notations for Redshift DMLs.
    decode :
        Map raw values from Redshift Data API to the Python
        ``str`` type, for ease of dealing.

    """

    __multibyte_is_allowed__ = True
    __redshift_name__ = "VARCHAR"
    __default_length__ = 256
    __max_length__ = 65535


class Text(VarChar):
    """A type for Redshift ``TEXT`` type. This type is alias for
    ``VARCHAR`` type, but the specification about length is different:
    the length is fixed as 256.

    Methods
    -------
    encode :
        Encode objects stored in a column for pandas.DataFrame
        to ``str``-typed notations for Redshift DMLs.
    decode :
        Map raw values from Redshift Data API to the Python
        ``str`` type, for ease of dealing.

    """

    __redshift_name__ = "TEXT"

    def __init__(self):
        """Construct the Redshift ``TEXT`` type."""

        self.length = self.__default_length__


class TimeStamp(RedshiftType):
    """A type for Redshift ``TIMESTAMP`` type.

    This type values are decoded with ``pandas.to_datetime``.

    The encoder for this type accepts any values which can be
    converted to datetime objects with ``pandas.to_datetime``.

    Methods
    -------
    encode :
        Encode objects stored in a column for pandas.DataFrame
        to ``str``-typed notations for Redshift DMLs.
    decode :
        Map raw values from Redshift Data API to the proper
        datetime.datetime type, for ease of dealing.

    """

    __redshift_name__ = "TIMESTAMP"
    __dt_format__ = "%Y-%m-%d %H:%M:%S"
    __utc__ = False

    def encode(self, col: Series) -> Series:
        """Encode objects stored in a column for pandas.DataFrame to
        ``str``-typed notations for Redshift DMLs.

        First, values are converted to datetime objects with
        ``pandas.to_datetime``.
        Next, values are converted to text with ``strftime`` and format.
        In the format, quote is included.

        Parameters
        ----------
        col : pandas.Series
            The column storing original objects in pandas.DataFrame.

        Returns
        -------
        encoded_col : pandas.Series
            Column storing Redshift notations.

        """

        def _strftime(obj):
            if pandas.isnull(obj) or pandas.isna(obj):
                return numpy.nan
            if hasattr(obj, "strftime"):
                return obj.strftime(self.__dt_format__)
            return obj

        # to deal with such types that is unable to convert to datetime
        # with ``pandas.to_datetime`` like Timea
        encoded_col = col.map(_strftime)
        try:
            encoded_col = pandas.to_datetime(encoded_col, utc=self.__utc__)
        except ParserError as err:
            raise TypeError("cannot parse to datetime {}".format(str(err)))

        output_format = "'{}'".format(self.__dt_format__)
        encoded_col = encoded_col.dt.strftime(output_format)
        encoded_col = encoded_col.fillna("NULL")

        return encoded_col

    def decode(self, col: Series) -> Series:
        """Raw values in response from Redshift data api are represented
        in str type. This decoder will convert raw values to datetime
        objects with ``pandas.to_datetime``.

        Parameters
        ----------
        col :
            Column storing raw values in response from Redshift Data API.

        Returns
        -------
        col :
            Column storing datetime.datetime values.

        """

        return pandas.to_datetime(col, errors="coerce", utc=self.__utc__)


class TimeStampTz(TimeStamp):
    """A type for Redshift ``TIMESTAMPTZ`` type.

    This type values are decoded with ``pandas.to_datetime`` and
    option ``utc``.

    The encoder for this type accepts any values which can be
    converted to datetime objects with ``pandas.to_datetime``.

    Methods
    -------
    encode :
        Encode objects stored in a column for pandas.DataFrame
        to ``str``-typed notations for Redshift DMLs.
    decode :
        Map raw values from Redshift Data API to the proper
        datetime.datetime type, for ease of dealing.

    """

    __redshift_name__ = "TIMESTAMPTZ"
    __dt_format__ = "%Y-%m-%d %H:%M:%S%z"
    __utc__ = True


class Date(TimeStamp):
    """A type for Redshift ``DATE`` type.

    This type values are decoded by converting to datetime objects
    with ``pandas.to_datetime`` and in addition converting to date
    objects by ``datetime.date()``.

    The encoder for this type accepts any values which can be
    converted to datetime objects with ``pandas.to_datetime``.

    Methods
    -------
    encode :
        Encode objects stored in a column for pandas.DataFrame
        to ``str``-typed notations for Redshift DMLs.
    decode :
        Map raw values from Redshift Data API to the proper
        datetime.date type, for ease of dealing.

    """

    __redshift_name__ = "DATE"
    __dt_format__ = "%Y-%m-%d"

    def decode(self, col: Series) -> Series:
        """Raw values in response from Redshift data api are represented
        in str type. First, this decoder converts raw values to datetime
        objects with ``pandas.to_datetime``. Next, datetime objects are
        converted to date objects with ``datetime.date()``.

        Parameters
        ----------
        col :
            Column storing raw values in response from Redshift Data API.

        Returns
        -------
        col :
            Column storing datetime.date values.

        """

        col = super().decode(col)
        return col.map(lambda dt: dt.date() if not pandas.isna(dt) else dt)


class Time(TimeStamp):
    """A type for Redshift ``TIME`` type.

    This type values are decoded by converting to datetime objects
    with ``pandas.to_datetime`` and in addition converting to time
    objects by ``datetime.time()``.

    The encoder for this type accepts any values which can be
    converted to datetime objects with ``pandas.to_datetime``.

    Methods
    -------
    encode :
        Encode objects stored in a column for pandas.DataFrame
        to ``str``-typed notations for Redshift DMLs.
    decode :
        Map raw values from Redshift Data API to the proper
        datetime.time type, for ease of dealing.

    """

    __redshift_name__ = "TIME"
    __dt_format__ = "%H:%M:%S"

    def decode(self, col: Series) -> Series:
        """Raw values in response from Redshift data api are represented
        in str type. This decoder will convert raw values to datetime
        objects with ``pandas.to_datetime``. Next, datetime objects are
        converted to time objects with ``datetime.time()``.

        Parameters
        ----------
        col :
            Column storing raw values in response from Redshift Data API.

        Returns
        -------
        col :
            Column storing datetime.time values.

        """

        col = super().decode(col)
        return col.map(lambda dt: dt.time() if not pandas.isna(dt) else dt)


class TimeTz(TimeStamp):
    """A type for Redshift ``TIMETZ`` type.

    This type values are decoded by converting to datetime objects
    with ``pandas.to_datetime`` and in addition converting to time
    objects by ``datetime.timetz()``.

    The encoder for this type accepts any values which can be
    converted to datetime objects with ``pandas.to_datetime``.

    Methods
    -------
    encode :
        Encode objects stored in a column for pandas.DataFrame
        to ``str``-typed notations for Redshift DMLs.
    decode :
        Map raw values from Redshift Data API to the proper
        datetime.time type, for ease of dealing.

    """

    __redshift_name__ = "TIMETZ"
    __dt_format__ = "%H:%M:%S%z"
    __utc__ = True

    def decode(self, col: Series) -> Series:
        """Raw values in response from Redshift data api are represented
        in str type. This decoder will convert raw values to datetime
        objects with ``pandas.to_datetime``. Next, datetime objects are
        converted to timetz objects with ``datetime.timetz()``.

        Parameters
        ----------
        col :
            Column storing raw values in response from Redshift Data API.

        Returns
        -------
        col :
            Column storing datetime.time values.

        """
        col = super().decode(col)
        return col.map(lambda dt: dt.timetz() if not pandas.isna(dt) else dt)


class Geometry(RedshiftType):
    """ "A type for Redshift ``GEOMETRY`` type.

    The decoder or encoder for this type has not been implemented.

    GEOMETRY values can be represented in ``str`` for both query
    response and DMLs.

    Methods
    -------
    encode :
        Encode objects stored in a column for pandas.DataFrame
        to ``str``-typed notations for Redshift DMLs.
    decode :
        Map raw values from Redshift Data API to Python
        ``str`` type, for ease of dealing.

    """

    __redshift_name__ = "GEOMETRY"


class Super(RedshiftType):
    """A type for Redshift ``SUPER`` type. This type is equivalent
    to Python generic types: ``int``, ``float``, ``str``, ``list``,
    ``dict``, and ``None``.

    This type values are decoded by converting to Python objects
    mainly with ``json.loads``.

    The encoder for this type accepts the six Python types mentioned
    above. The semi-structured data, ``list`` and ``dict`` types,
    are parsed with ``JSON_PARSE`` before ingestion to Redshift.

    This type is sub-class for ``VarChar`` to leverage the private
    method ``_encode_text``.

    Methods
    -------
    encode :
        Encode objects stored in a column for pandas.DataFrame
        to ``str``-typed notations for Redshift DMLs.
    decode :
        Map raw values from Redshift Data API to the proper
        Python object type, for ease of dealing.

    """

    __redshift_name__ = "SUPER"

    def __init__(self):
        """Construct the Redshift ``SUPER`` type."""
        self.length = 0

    def encode(self, col: Series) -> Series:
        """Encode objects stored in a column for pandas.DataFrame to
        ``str``-typed notations for Redshift DMLs.

        The behavior depends on the type for the value. 1. The ``int``
        or ``float`` values are casted to ``str``, 2. ``str`` values
        are transformed with the same logic as ``CHAR``/``VARCHAR``-type
        encoder, 3. ``list`` or ``dict`` values are once transformed as
        ``str`` values are done and the transformed values are set as
        the argument for ``JSON_PARSE``.

        Parameters
        ----------
        col : pandas.Series
            The column storing original objects in pandas.DataFrame.

        Returns
        -------
        encoded_col : pandas.Series
            Column storing Redshift notations.

        """

        def _encode_super(obj):
            if obj is None:
                return "NULL"
            elif type(obj) is int or type(obj) is float:
                if pandas.isna(obj):
                    return "NULL"
                return str(obj)
            elif type(obj) is str:
                return self._encode_text(obj)
            elif type(obj) is dict or type(obj) is list:
                json_str = json.dumps(obj)
                encoded_json = self._encode_text(json_str)
                return "JSON_PARSE({})".format(encoded_json)
            else:
                raise TypeError(
                    "unsupported datatype {} for SUPER type".format(type(obj))
                )

        encoded_col = col.astype("object")
        encoded_col = col.map(_encode_super)
        return encoded_col

    def decode(self, col: Series) -> Series:
        """Raw values in response from Redshift data api are represented
        in str type. This decoder will convert raw values to Python
        objects with ``json.loads``.

        Parameters
        ----------
        col :
            Column storing raw values in response from Redshift Data API.

        Returns
        -------
        col :
            Column storing Python object values.

        """

        def _decode_super(obj):
            if pandas.isna(obj):
                return numpy.nan

            return json.loads(obj)

        return col.map(_decode_super)
