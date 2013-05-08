#!/usr/bin/env python

import sys
with_svgviewer = ("--with-svgviewer" in sys.argv)
if with_svgviewer:
    sys.argv = [x for x in sys.argv if x != "--with-svgviewer"]

import os
from distutils.core import setup, Extension
from distutils.command.build_ext import build_ext

import augustus.version

try:
    import numpy
    import lxml
except ImportError:
    sys.stderr.write("Augustus requires numpy and lxml:\n    sudo apt-get install python-numpy python-lxml\n")
    sys.exit(-1)

class svgviewer_build_ext(build_ext):
    def build_extension(self, extension):
        try:
            build_ext.build_extension(self, extension)
        except:
            sys.stderr.write("Could not build svgviewer extension, possibly because the prerequisites have not been installed:\n")
            sys.stderr.write("    sudo apt-get install python-dev libgtk2.0-dev libglib2.0-dev librsvg2-dev libcairo2-dev\n")
            sys.exit(-1)

def svgviewer_pkgconfig():
    return [x for x in os.popen("pkg-config --cflags --libs gtk+-2.0 gthread-2.0 librsvg-2.0").read().split(" ") if x.strip() != ""]

svgviewer_extension = Extension(os.path.join("augustus", "svgviewer"),
                                [os.path.join("augustus", "svgviewer.c")], {},
                                libraries=["cairo", "rsvg-2"],
                                extra_compile_args=svgviewer_pkgconfig(),
                                extra_link_args=svgviewer_pkgconfig())

# find all subpackages using `augustus -name '*.py' | sed 's/\/[^\/]*$//' | sed 's/\//./g' | sort | uniq`

setup(name="augustus",
      version=augustus.version.__version__,
      description="Augustus: a library for evaluating, producing, and manipulating statistical and data mining models in PMML",
      author="Open Data Group",
      author_email="support@opendatagroup.com",
      url="http://augustus.googlecode.com",
      download_url="http://code.google.com/p/augustus/downloads/list",
      packages=["augustus",
                "augustus.core",
                "augustus.core.plot",
                "augustus.jython",
                "augustus.jython.lxml",
                "augustus.mapreduce",
                "augustus.pmml",
                "augustus.pmml.expression",
                "augustus.pmml.model",
                "augustus.pmml.model.baseline",
                "augustus.pmml.model.clustering",
                "augustus.pmml.model.segmentation",
                "augustus.pmml.model.trees",
                "augustus.pmml.odg",
                "augustus.pmml.plot",
                "augustus.pmml.predicate",
                "augustus.producer",
                "augustus.producer.kmeans",
                ],
      package_data={"augustus.core": ["*.xsd", "*.xslt"]},
      classifiers=["Development Status :: 4 - Beta",
                   "Environment :: Console",
                   "Intended Audience :: Developers",
                   "Intended Audience :: Financial and Insurance Industry",
                   "Intended Audience :: Information Technology",
                   "Intended Audience :: Science/Research",
                   "License :: OSI Approved :: Apache Software License",
                   "Operating System :: POSIX",
                   "Operating System :: UNIX",
                   "Operating System :: Microsoft :: Windows",
                   "Programming Language :: Python",
                   "Topic :: Scientific/Engineering :: Information Analysis",
                   "Topic :: Scientific/Engineering :: Mathematics",
                   ],
      cmdclass={"build_ext": svgviewer_build_ext},
      ext_modules=([svgviewer_extension] if with_svgviewer else []),
      )
