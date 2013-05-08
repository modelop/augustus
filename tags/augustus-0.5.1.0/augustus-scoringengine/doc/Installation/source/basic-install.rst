.. raw:: latex

    \newpage


************
Installation
************

Please refer to :file:`INSTALL` which ships with the Augustus release if you
have any questions regarding installation and do not want to read this document.
The file is brief.

Operating system compatibility
==============================

Augustus is written in Python; it should run on any system with the required
Python environment.  Augustus 0.5 has been tested on:

    * Ubuntu Releases 10.04 - 11.04
    * Debian 4 and 5 (etch and lenny)
    * AIX 5

Older versions have been tested and used on Windows XP, Open Solaris, and
earlier releases of Ubuntu.

Platform-specific instructions are available: see the sections
:ref:`Installation-on-Ubuntu`, :ref:`Installation-on-RHEL`,
:ref:`Installation-on-Darwin`, and :ref:`Installation-on-Windows`

.. _`Augustus-package` :

Obtaining Augustus
==================

Download
--------
The most recent Augustus release can be downloaded from
`<http://code.google.com/p/augustus/downloads/>`_. Search for "release".
Unpack the distribution in the directory of your choice and expand.  For
example::

    $ cd /home/odg
    $ wget http://augustus.googlecode.com/files/Augustus-<VERSION>.tar.gz
    $ tar zxvf Augustus-<VERSION>.tar.gz

As of release 0.5, the archives only contain the scoring engine,
:file:`augsutus-scoringengine`, and the examples, :file:`augustus-examples`, and
not the tests, :file:`augustus-tests`.  If you want the tests, then you must
check them out from source control.

.. index:: SVN checkout

Source checkout
---------------

It is also possible to check out a read-only copy of the trunk via Google Code's
Subversion::

    $ cd /home/odg
    $ svn checkout \
        http://augustus.googlecode.com/svn/trunk/ augustus-trunk-read-only

Branches and tagged releases are available from source control as well as
downloads.

Checking out the trunk provides the scoring engine, tests, and examples.  If you
only want the scoring engine, then use::

    $ cd /home/odg
    $ svn co http://augustus.googlecode.com/svn/trunk/augustus-scoringengine \
        augustus-scoringengine

This saves a great deal of disk space.

.. index:: Operating systems, Linux distributions, Compilation tools

Dependencies
============

Required
--------

.. index:: Python version

Python
++++++

Augustus is written in Python. Python can be downloaded from
`<http://www.python.org/>`_.  We strive to test Augustus against Python 2.5,
2.6, and 2.7 and any issues encountered running against those version should be
reported as bugs.  Augustus is not supported for any other versions.

.. index:: Python packages, NumPy

NumPy
+++++

*Required*. NumPy 1.2.1 or 1.3.0 are suggested, and Augustus is tested against
those versions.  NumPy 1.0 will not work because the ``numpy.ma`` module is in a
different location.  NumPy through version 1.5 should work.   NumPy is available
for many operating systems via their package manager.  If it is not available on
your system, it can be obtained from: `<http://www.scipy.org/Download>`_


Setup (general)
===============

If you have downloaded or checked out Augustus and Python and NumPy are already
installed, then all that is left is to make Augustus accessible via the
:envvar:`PYTHONPATH` and :envvar:`PATH` environment variables.
This can be done by 1) manually adding the Augustus directory to each of the above
environment variables, or 2) running the package's :file:`setup.py` script to
install Augustus in default locations.  Instructions for both follow below.

Using :file:`setup.py`
----------------------

Augustus comes with a setup file.  By default, it installs into locations
already in the environment variables :envvar:`PYTHONPATH` and :envvar:`PATH`.
To use it::
  
    $ cd augustus-scoringengine
    $ sudo python setup.py install

For options using ``setup.py``, type ``python setup.py --help``.  If you do not
have ``sudo`` privileges, an alternate install directory can be passed to
``setup.py``.  By default, if using Python 2.6, ``python setup.py install``
installs Augustus under the directories::
  
    /usr/local/lib/python2.6
    /usr/local/bin

As an example, installing Augustus using ``setup.py`` on Ubuntu creates::

    /usr/local/lib/python2.6/dist-packages/augustus
    /usr/local/lib/python2.6/dist-packages/Augustus-0.5.0.0.egg-info

and places under ``/usr/local/bin`` the files::

    AnalysisWorkflow    PmmlSplit
    Augustus            ScoresAwk
    munge               ScoresDiff
    PmmlDiff            ScoresDiffFast
    PmmlSed             unitable

Setting environment variables
------------------------------------------

If a tarball is unpacked, the source checked out, or :file:`setup.py` was sent
to a directory not in the System paths, then the variables :envvar:`PYTHONPATH`
and :envvar:`PATH` need to be updated.

For example, if :file:`augustus-scoringengine` is in :file:`/home/odg/`, then 
add these lines to your :file:`.bashrc` or equivalent file::
  
    PYTHONPATH=$PYTHONPATH:/home/odg/augustus-scoringengine
    PATH=$PATH:/home/odg/augustus-scoringengine/augustus/bin

.. raw:: latex

    \newpage

Platform-specific setup
=======================

.. toctree::
    :maxdepth: 2

    basic-install-Ubuntu.rst
    basic-install-RHEL.rst
    basic-install-Darwin.rst
    basic-install-Windows.rst


.. raw:: latex

    \newpage

**************
Using Augustus
**************

Augustus is now installed.  See the Augustus Primer, included with the
documentation and available from the project website, for a walk through of
the examples.

If you want to remove Augustus from your system and you are running from the
source, simply delete the directory::

    $ rm -rf augustus-scoringengine

If Augustus was installed using ``setup.py``, you can remove Augustus from the
system by finding the directory and egg created by ``setup.py`` and removing
them.

.. raw:: latex

    \newpage

.. similar to::
    {augustus}/INSTALL
    augustus-installation.rst
    augustus-installation-Darwin.rst
    augustus-installation-RHEL.rst
    augustus-installation-Ubuntu.rst
    augustus-installation-Windows.rst
    http://code.google.com/p/augustus/wiki/Installation
    https://sites.google.com/a/opendatagroup.com/augustus-training/original-training-docs-2008
        -- file: Augustus0_2_6_5_033108.ppt
