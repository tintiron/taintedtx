.. taintedtx documentation master file, created by
   sphinx-quickstart on Tue Sep 14 12:29:28 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to taintedtx's documentation!
=====================================
**TaintedTX** is a python library tool for Bitcoin-like cryptocurrencies tracking analysis. The tool currently uses Pandas data frame module as its primary method of operation.

We include Bitcoin sample data in the sampledata folder that contains the complete first 2 years and only the first day for the years 2018 and 2019. We also include a dummy database in testdatabase that we build for our early testing of the library.

Note that it is possible to create transaction data frames directly from blocksci without having to save them into files for reading.

To test the library tool online, visit: https://mybinder.org/v2/gh/tintiron/taintedtx/HEAD The binder may take a while to load (roughly 10 minutes).

Feel free to report bugs in issue or send an email to tt28@hw.ac.uk

Contents
-------
.. toctree::
   example-usage
   reference/reference
   :maxdepth: 2
   :caption: Contents:
   


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
