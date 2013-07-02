#!/usr/bin/env python

import sys
import os
from distutils.core import setup, Extension
from distutils.command.build_ext import build_ext
import getopt

import augustus.version

################################################ setup

with_svgviewer = ("--with-svgviewer" in sys.argv)
if with_svgviewer:
    sys.argv = [x for x in sys.argv if x != "--with-svgviewer"]

with_avrostream = ("--with-avrostream" in sys.argv)
if with_avrostream:
    sys.argv = [x for x in sys.argv if x != "--with-avrostream"]

try:
    import numpy
    import lxml
except ImportError:
    sys.stderr.write("Augustus requires numpy and lxml:\n    sudo apt-get install python-numpy python-lxml\n")
    if not raw_input("Continue anyway? (y/N) ").lower().startswith("y"):
        sys.exit(-1)

ext_modules = []

################################################ optional svgviewer

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

if with_svgviewer:
    ext_modules.append(Extension(os.path.join("augustus", "svgviewer"),
                                 [os.path.join("augustus", "svgviewer.c")], {},
                                 libraries=["cairo", "rsvg-2"],
                                 extra_compile_args=svgviewer_pkgconfig(),
                                 extra_link_args=svgviewer_pkgconfig()))

################################################ optional avrostream

AVRO_HOME = "/opt/avrocpp"
BOOST_INCLUDEDIR = "/usr/include/boost"
BOOST_LIBRARYDIR = "/usr/lib"
otherargs = []
for arg in sys.argv:
    if arg.find("avro-home") != -1 or arg.find("boost-include") != -1 or arg.find("boost-lib") != -1:
        optlist, argv = getopt.getopt([arg], "", ["avro-home=", "boost-include=", "boost-lib="])
        for name, value in optlist:
            if name == "--avro-home":
                AVRO_HOME = value
            if name == "--boost-include":
                BOOST_INCLUDEDIR = value
            if name == "--boost-lib":
                BOOST_LIBRARYDIR = value
    else:
        otherargs.append(arg)
sys.argv = otherargs
AVRO_INCLUDE = os.path.join(AVRO_HOME, "include")
AVRO_LIB = os.path.join(AVRO_HOME, "lib")

if with_avrostream:
    ext_modules.append(Extension(os.path.join("augustus", "dataio", "avrostream"),
                                 [os.path.join("augustus", "dataio", "avrostream.cpp")],
                                 library_dirs=[AVRO_LIB, BOOST_LIBRARYDIR],
                                 libraries=["avrocpp"],
                                 include_dirs=[AVRO_INCLUDE, os.path.join(AVRO_INCLUDE, "avro"), BOOST_INCLUDEDIR, numpy.get_include()],
                                 ))

################################################ Augustus itself

setup(name="augustus",
      version=augustus.version.__version__,
      description="Augustus: a library for evaluating, producing, and manipulating statistical and data mining models in PMML",
      author="Open Data Group",
      author_email="support@opendatagroup.com",
      url="http://augustus.googlecode.com",
      download_url="http://code.google.com/p/augustus/downloads/list",
      packages=["augustus",     # find all subpackages using `augustus -name '*.py' | sed 's/\/[^\/]*$//' | sed 's/\//./g' | sort | uniq`
                "augustus.core",
                "augustus.core.plot",
                "augustus.dataio",
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
      ext_modules=ext_modules,
      )
