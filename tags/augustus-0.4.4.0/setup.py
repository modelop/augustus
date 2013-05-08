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
import augustus.const as AUGUSTUS_CONST

# System check
import augustus.const as CONST
CONST.check_python_version()
# End check

setup(

  name='Augustus',
  version=AUGUSTUS_CONST._AUGUSTUS_VER,
  description='Augustus - A scoring engine for statistical and data mining models based on the Predictive Model Markup Language (PMML) version ' + AUGUSTUS_CONST._PMML_VER,
  long_description=__doc__,
  author='Open Data Group',
  author_email='support@opendatagroup.com',
  license='GPLv2',
  url='http://augustus.googlecode.com',
  download_url='http://code.google.com/p/augustus/downloads/list',
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
    'augustus.modellib.clustering',
    'augustus.modellib.clustering.producer',
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
    'bin/AugustusClusteringProducer',
    'bin/userInitializeConfigs',
    'bin/userBuildMySQL',
    'bin/userInitializeModels',
    'bin/fake_score_handler',
    'bin/fake_event_source',
  ],
  classifiers=[
    'Development Status :: 4 - Beta',
    'Environment :: Console',
    'Intended Audience :: Developers',
    'Intended Audience :: Financial and Insurance Industry',
    'Intended Audience :: Information Technology',
    'Intended Audience :: Science/Research',
    'License :: OSI Approved :: GNU General Public License (GPL) version 2',
    'Operating System :: POSIX',
    'Operating System :: UNIX',
    'Operating System :: Microsoft :: Windows',
    'Programming Language :: Python',
    'Topic :: Scientific/Engineering :: Information Analysis',
    'Topic :: Scientific/Engineering :: Mathematics',
  ],
)
