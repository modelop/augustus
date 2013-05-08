.. raw:: latex

    \newpage


.. index:: Installation, RHEL, Installation from source

.. _`installation-on-RHEL`:

Installation on RHEL 5.5
------------------------

This section shows the installation of Augustus trunk from source on a Amazon
EC2 instance.  The steps were performed using::

    AMI Id: ami-2632cc4f
    Zone:   us-east-1c
    Type:   m1.large
    Owner:  03269352882


The instance comes with the default version of Python for the OS installed and
without NumPy::

    # cat /etc/redhat-release
    Red Hat Enterprise Linux Server release 5.5 (Tikanga)
    # python -V
    Python 2.4.3

    # python -c "from numpy import *"
    Traceback (most recent call last):
    File "<string>", line 1, in ?
    ImportError: No module named numpy


.. note:: **Expired ssl certificates**

    Due to expired ssl certificates on the Red Hat side, Amzon instances for
    both RHEL 5 and 6 need to perform an update.  For the zone / release we
    used, this is:

    # rpm -Uhv
    # http://redhat-clientconfig-us-east-1.s3.amazonaws.com/rh-amazon-rhui-client-2.2.16-1.el5.noarch.rpm
    # yum clean all

    This is temporary an more information is available at:

    https://forums.aws.amazon.com/thread.jspa?threadID=76738&tstart=0&messageID=280829


Step 1.  Install Python 2.6

Check for python26 packages from the package manager::

    # sudo yum list *python26*

    ... [omitted]   

    Error: No matching Packages to list

Add EPEL to the list of hosts::

    # su -c 'rpm -Uvh \
    # http://download.fedora.redhat.com/pub/epel/5/x86_64/epel-release-5-4.noarch.rpm'

Check again for python26 packages from the package manager, it should now be
available::

    # sudo yum list python26
    Loaded plugins: amazon-id, fastestmirror, rhui-lb, security
    Loading mirror speeds from cached hostfile

    ... [omitted]   

    Available Packages
    python26.x86_64    2.6.5-6.el    epel

Install it using yum::

    # yum install python26.x86_64
    Loaded plugins: amazon-id, fastestmirror, rhui-lb, security
    Loading mirror speeds from cached hostfile

    ... [omitted]

    Installed:
      python26.x86_64 0:2.6.5-6.el5

    Dependency Installed:
      libffi.x86_64 0:3.0.5-1.el5    python26-libs.x86_64 0:2.6.5-6.el5

    Complete!

Test the Installation, both Python versions should be available::

    # python -V
    Python 2.4.3

    # python26 -V
    Python 2.6.5


Step 2. Get NumPy from EPEL

Find the package and install it::

    # yum list available | grep numpy.x86_64
    python26-numpy.x86_64    1.5.1-5.el5    epel
    

    # yum install python26-numpy.x86_64
    Loaded plugins: amazon-id, fastestmirror, rhui-lb, security
    Loading mirror speeds from cached hostfile
     * epel: mirror.cogentco.com

    ... [omitted]

    Installed:
      python26-numpy.x86_64 0:1.5.1-5.el5

    Dependency Installed:
      atlas.x86_64 0:3.8.3-1.el5    libgfortran.x86_64 0:4.1.2-51.el5

    Complete!

Test the NumPy was installed for the correct version of Python.  Augustus
will not run with Python 2.4, so the NumPy installation has to be for Python 2.6::

    # python -c "import numpy"
    Traceback (most recent call last):
      File "<string>", line 1, in ?
    ImportError: No module named numpy

    # python26 -c "import numpy"


Step 3. Get Augustus and install it using :file:`setup.py`::

    # svn checkout \
        http://augustus.googlecode.com/svn/trunk/augustus-scoringengine augustus-scoringengine

    ... [omitted]

    Checked out revision 527

    # cd augustus-scoringengine/
    # python26 setup.py install
    running install
    running build
    
    ... [omitted]

    running install_egg_info
    Writing /usr/lib/python2.6/site-packages/Augustus-0.5.0.0-py2.6.egg-info

Verify the Installation::

    # cd /tmp
    # python26
    Python 2.6.5 (r265:79063, Feb 28 2011, 21:55:45) 
    [GCC 4.1.2 20080704 (Red Hat 4.1.2-50)] on linux2
    Type "help", "copyright", "credits" or "license" for more information.
    >>> from augustus import *
    >>>

Augustus is now installed.

