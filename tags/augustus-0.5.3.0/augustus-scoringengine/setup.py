#!/usr/bin/env python

"""
Augustus - A scoring engine for statistical and data mining models
based on the Predictive Model Markup Language (PMML).
"""

__copyright__ = """
Copyright (C) 2006-2011  Open Data ("Open Data" refers to
one or more of the following companies: Open Data Partners LLC,
Open Data Research LLC, or Open Data Capital LLC.)

This file is part of Augustus.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from distutils.core import setup
from augustus import version
from augustus.version import __version__

# System check
version._python_check()

setup(

  name='Augustus',
  version=__version__,
  description='Augustus - A scoring engine for statistical and data mining models based on the Predictive Model Markup Language (PMML) version 4.1',
  long_description=__doc__,
  author='Open Data Group',
  author_email='support@opendatagroup.com',
  url='http://augustus.googlecode.com',
  download_url='http://code.google.com/p/augustus/downloads/list',
  packages=['augustus',
    'augustus',
    'augustus.algorithms',
    'augustus.core',
    'augustus.datastreams',
    'augustus.engine',
    'augustus.logsetup',
    'augustus.unitable',
    'augustus.tools',
    'augustus.applications',
  ],
  package_data={
    'augustus.pmmllib': ['xsd/*.xsd'],
  },
  scripts=[
    'augustus/bin/Augustus',
    'augustus/bin/munge',
    'augustus/bin/PmmlDiff',
    'augustus/bin/PmmlSed',
    'augustus/bin/PmmlSplit',
    'augustus/bin/ScoresDiff',
    'augustus/bin/ScoresDiffFast',
    'augustus/bin/ScoresAwk',
    'augustus/bin/AnalysisWorkflow',
    'augustus/bin/ConvertTable',
  ],
  classifiers=[
    'Development Status :: 4 - Beta',
    'Environment :: Console',
    'Intended Audience :: Developers',
    'Intended Audience :: Financial and Insurance Industry',
    'Intended Audience :: Information Technology',
    'Intended Audience :: Science/Research',
    'License :: OSI Approved :: Apache Software License',
    'Operating System :: POSIX',
    'Operating System :: UNIX',
    'Operating System :: Microsoft :: Windows',
    'Programming Language :: Python',
    'Topic :: Scientific/Engineering :: Information Analysis',
    'Topic :: Scientific/Engineering :: Mathematics',
  ],
)
