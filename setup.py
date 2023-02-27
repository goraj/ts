from setuptools import setup

setup(
    name="ts",
    version="",
    packages=["ts"],
    url="",
    license="",
    author="Jacob Gora",
    author_email="",
    description="",
    install_requires=[
        "pandas",
        "pytest",
        "pyarrow",
        "polars>=0.14.29",
        "pre-commit",
        "black",
        "pytype",
    ],
)
