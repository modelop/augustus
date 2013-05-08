#!/usr/bin/env python

"""
Augustus - A scoring engine for statistical and data mining models
based on the Predictive Model Markup Language (PMML).
"""

__copyright__ = """
Copyright (C) 2005-2009  Open Data ("Open Data" refers to
one or more of the following companies: Open Data Partners LLC,
Open Data Research LLC, or Open Data Capital LLC.)

This file is part of Augustus.

Augustus is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA
"""

from distutils.core import setup

setup(
  name='augustus',
  version='0.3.3.1',
  description='Augustus - A scoring engine based on PMML',
  long_description=__doc__,
  author='Open Data Group',
  author_email='support@opendatagroup.com',
  license='GPL',
  url='http://sourceforge.net/projects/augustus/',
  download_url='http://downloads.sourceforge.net/augustus/',
  platforms='python2.5',
  packages=['augustus',
    'augustus.pmmllib',
    'augustus.external',
    'augustus.kernel',
    'augustus.kernel.unitable',
    'augustus.kernel.unitable.veclib',
    'augustus.kernel.unitable.veclib.base',
    'augustus.modellib',
    'augustus.modellib.baseline',
    'augustus.modellib.baseline.producer',
    'augustus.modellib.baseline.tools',
    'augustus.modellib.naive_bayes',
    'augustus.modellib.naive_bayes.producer',
    'augustus.modellib.tree',
    'augustus.modellib.tree.producer',
    'augustus.modellib.regression',
    'augustus.modellib.regression.producer',
    'augustus.runlib',
    'augustus.tools',
  ],
  package_data={
    'augustus.pmmllib': ['xsd/*.xsd'],
  },
  scripts=[
    'bin/__setpath.py',
    'bin/unitable',
    'bin/munge',
    'bin/runfifo',
    'bin/realpmml',
    'bin/AugustusPMMLConsumer',
    'bin/AugustusBaselineProducer',
    'bin/AugustusNaiveBayesProducer',
    'bin/AugustusTreeProducer',
    'bin/AugustusRegressionProducer',
    'bin/userInitializeConfigs',
    'bin/userBuildMySQL',
    'bin/userInitializeModels',
    'bin/fake_score_handler',
    'bin/fake_event_source',
  ],
  classifiers=[
    'Development Status :: 4 - Beta',
    #'Development Status :: 5 - Production/Stable',
    'Environment :: Console',
    'Intended Audience :: Developers',
    'Intended Audience :: Financial and Insurance Industry',
    'Intended Audience :: Information Technology',
    'Intended Audience :: Science/Research',
    'License :: OSI Approved :: GNU General Public License (GPL)',
    'Operating System :: POSIX',
    'Operating System :: Microsoft :: Windows',
    'Programming Language :: Python',
    'Topic :: Scientific/Engineering :: Information Analysis',
    'Topic :: Scientific/Engineering :: Mathematics',
  ],
)

#################################################################
# vim:sw=2:sts=2:expandtab:shiftround
