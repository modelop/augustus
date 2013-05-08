
.. index:: Installation, Ubuntu

.. _`installation-on-Ubuntu`:

Installation on Ubuntu
----------------------

This section shows the installation of Augustus trunk on a new Ubuntu 10.04.3
LTS installation with up-to-date packages. 

Ubuntu 10.04.3 uses Python 2.6.5, NumPY 1.3.0 and subversion 1.6.6 and all are
available from APT. These default versions are used when acquiring the packages
in the session below.

The example starts with modifying the bash prompt to include the timestamp-- the
entire install takes three minutes.

::

    odg@ubuntu:~$ export PS1='\u@\t:\w\$ '
    odg@19:58:32:~$ python --version
    Python 2.6.5
    odg@19:58:41:~$ python -c "import numpy"
    Traceback (most recent call last):
      File "<string>", line 1, in <module>
    ImportError: No module named numpy
    odg@19:58:52:~$ sudo apt-get install python-numpy
    Reading package lists... Done
    Building dependency tree       
    Reading state information... Done

    [output omitted ...]

    Need to get 4,784kB of archives.
    After this operation, 14.8MB of additional disk space will be used.
    Do you want to continue [Y/n]? Y

    [output omitted ...]

    odg@19:59:14:~$ python
    Python 2.6.5 (r265:79063, Apr 16 2010, 13:09:56) 
    [GCC 4.4.3] on linux2
    Type "help", "copyright", "credits" or "license" for more information.

::

    >>> import numpy
    >>> numpy.__version__
    '1.3.0'
    >>> exit()
   
::   

    odg@19:59:38:~$ svn
    The program 'svn' is currently not installed.  You can install it by typing:
    sudo apt-get install subversion
    odg@19:59:43:~$ sudo apt-get install subversion
    Reading package lists... Done
    Building dependency tree       
    Reading state information... Done

    [output omitted ...]

    After this operation, 6,836kB of additional disk space will be used.
    Do you want to continue [Y/n]? Y

    [output omitted ...]

    odg@20:00:05:$ svn checkout \
    > http://augustus.googlecode.com/svn/trunk/augustus-scoringengine augustus-scoringengine

    [output omitted ...]

    Checked out revision 527.

    odg@20:00:49:~$ cd augustus-scoringengine/
    odg@20:00:59:~/augustus-scoringengine$ sudo python setup.py install
    running install

    [output omitted ...]

    running install_egg_info
    Writing /usr/local/lib/python2.6/dist-packages/Augustus-0.5.0.pre.egg-info

    odg@20:01:07:~/augustus-scoringengine$ cd /tmp/
    odg@20:01:13:/tmp$ python -c "import augustus"
    odg@20:01:21:/tmp$ 

With minor differences in the default versions of Python and
Subversion, this installation process is the same with Ubuntu 9.10, 10.10, and
11.04.


