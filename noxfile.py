"""Nox test automation configuration.

See: https://nox.readthedocs.io/en/latest/
"""

import os
import os.path
import shutil

import nox


SUPPORTED_PYTHONS = ["3.7", "3.8", "3.9"]
SYSTEM_TEST_PYTHONS = ["3.7", "3.8", "3.9"]
LATEST_PYTHON = "3.7"

# Use a consistent version of black so CI is deterministic.
# Should match Stickler: https://stickler-ci.com/docs#black
BLACK_PACKAGE = "black==20.8b1"


@nox.session(python=LATEST_PYTHON)
def lint(session):
    session.install(BLACK_PACKAGE, "flake8")
    session.run("flake8", "pandas_redshift")
    session.run("flake8", "boto3_mock")
    session.run("flake8", "tests")
    session.run("flake8", "samples")
    session.run("black", "--check", ".")


@nox.session(python=LATEST_PYTHON)
def blacken(session):
    session.install(BLACK_PACKAGE)
    session.run("black", ".")


@nox.session(python=SUPPORTED_PYTHONS)
def unit(session):
    session.install("jsonschema", "pytest", "pytest-cov")
    session.install(
        "-e",
        ".",
        # Use dependencies versions from constraints file. This enables testing
        # across a more full range of versions of the dependencies.
        "-c",
        os.path.join(".", "ci", "constraints-{}.pip".format(session.python)),
    )
    session.run(
        "pytest",
        os.path.join(".", "tests", "unit"),
        "-v",
        "--cov=pandas_redshift",
        "--cov=boto3_mock",
        "--cov=tests.unit",
        "--cov-report",
        "xml:/tmp/pytest-cov.xml",
        *session.posargs
    )


@nox.session(python=LATEST_PYTHON)
def cover(session):
    session.install("coverage", "pytest-cov")
    session.run("coverage", "report", "--show-missing", "--fail-under=73")
    session.run("coverage", "erase")


@nox.session(python=LATEST_PYTHON)
def docs(session):
    """Build the docs."""

    session.install("numpydoc", "pandas", "sphinx", "sphinx-rtd-theme")
    session.install("-e", ".")

    shutil.rmtree(os.path.join("docs", "_build"), ignore_errors=True)
    session.run(
        "sphinx-build",
        "-W",  # warnings as errors
        "-T",  # show full traceback on exception
        "-N",  # no colors
        "-b",
        "html",
        "-d",
        os.path.join("docs", "_build", "doctrees", ""),
        os.path.join("docs", "source", ""),
        os.path.join("docs", "_build", "html", ""),
    )

@nox.session(python=SYSTEM_TEST_PYTHONS)
def system(session):
    session.install("pytest", "pytest-cov", "wget")
    session.install(
        "-e",
        ".",
        # Use dependencies versions from constraints file. This enables testing
        # across a more full range of versions of the dependencies.
        "-c",
        os.path.join(".", "ci", "constraints-{}.pip".format(session.python)),
    )

    additional_args = list(session.posargs)

    session.run(
        "pytest",
        os.path.join(".", "tests", "system"),
        "-v",
        *additional_args,
    )
