.. raw:: latex

    \newpage

.. index:: Installation, Darwin

.. _`installation-on-Darwin`:

Installation on Mac (Darwin)
----------------------------

Mac OS X has shipped with Python and NumPy installed since version 10.5 (Darwin
version 9.0).  A list of all Python packages that ship with Mac OS X is at:

.. raw:: latex

    \par

`<http://developer.apple.com/library/mac/#documentation/Cocoa/Conceptual/RubyPythonCocoa/Articles/RubyPythonMacOSX.html>`_.

This section shows the installation of Augustus trunk on Darwin version
10.7.0, which ships with :program:`Python 2.6.1`, :program:`NumPy 1.2.1`, and
:program:`Subversion 1.6.15`.

Download
^^^^^^^^

Download a release of Augustus from the Google code website
`<http://code.google.com/p/augustus/downloads/>`_.  When the download completes,
click on the TAR icon |tar_icon| to unpack the file where it was downloaded.

.. |tar_icon| image:: IMG/tar_icon.png
              :height: 14pt

Open a Terminal shell by navigating to
:menuselection:`Applications --> Utilities --> Terminal`, or by clicking on the
|magnify| icon on the top right and searching for 'terminal'.

.. |magnify| image:: IMG/magnify.png
             :height: 14pt

In the terminal, change directories to the Downloads folder, or the folder where Augustus
was unpacked.  If necessary, move the folder.

Checkout
^^^^^^^^
:program:`Subversion` ships with Mac OS X, and can be used from the command line
to check out Augustus::

    $ svn checkout \
      http://augustus.googlecode.com/svn/trunk/ augustus-trunk-read-only

If you prefer a GUI, the application :program:`svnX` is available directly from
Apple without charge.

Installation
^^^^^^^^^^^^

The installation can now proceed according to the general instructions in the
section :ref:`Augustus-package`.

.. note::
    If ``setup.py`` is used, the default install location is
    :file:`/Library/Python/2.6/site-packages/`.

.. note::
    If you have previously installed :program:`Macports` using default
    settings, a file :file:`.bash_profile` is in your home directory.
    Environment variables can be set there or in  your :file:`.bashrc`
    or equivalent shell file.
