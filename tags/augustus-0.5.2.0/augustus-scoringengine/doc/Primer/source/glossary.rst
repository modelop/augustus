.. raw:: latex

    \newpage


Glossary
========

.. glossary::

    Augustus
        A :term:`PMML`-compliant software toolkit written in Python.
        Augustus is an open source system for building and scoring
        statistical models designed to work with data sets that are too
        large to fit into memory

    Baseline Model
        Augustus uses the Baseline model element in PMML to describe
        a change detection method.  The user selects a distribution
        that describes the data under normal conditions, then chooses
        a test type, a test statistic, and a threshold value.
        The Augustus PMML consumer would score the data using the given
        test, and if instructed, raise a flag when the test statistic
        exceeds the selected threshold.

    Baseline Producer
        Augustus's Baseline Producers creates a PMML baseline model from
        three inputs: a configuration file, :term:`PMML Skeleton` File,
        and a set of training data.  It uses the training data to 
        determine the baseline mean (and if used, the variance) for
        each section of data, called a :term:`segment`.

    big data
        Data that are too large to be effectively processed using
        many statistical tools because the data cannot fit
        in memory or the database does not fit on a single computer.
        For example: the network traffic on a popular web host, a
        time series of temperature, position, and population
        data for all algae species measured on every square foot of the
        surface of the ocean, or all of the trades for that occurred
        at a particular exchange for a portfolio of symbols during a
        given time period.

    CSV 
        An acronym for the Comma Separated Variable files. Augustus
        can read :file:`.csv` files with any delimiter; it 
        uses its :term:`UniTable` to deduce the column separator
        from the first line of the file. The first line must be
        a header row.

    CUSUM
        The CUSUM statistical test is a change detection test that
        measures the difference between a reference distribution
        and the data set of a new distribution.
        More details are available on wikipedia at
        `<http://en.wikipedia.org/wiki/Cusum>`_.

    DATATYPE
        This is a list of base data types defined in PMML:
        ``string``, ``integer``, ``float``, ``double``, ``boolean``,
        ``date``, ``time``, ``dateTime``, ``dateDaysSince[0]``,
        ``dateDaysSince[1960]``, ``dateDaysSince[1970]``,
        ``dateDaysSince[1980]``, ``timeSeconds``, ``dateTimeSecondsSince[0]``,
        ``dateTimeSecondsSince[1960]``, ``dateTimeSecondsSince[1970]``,
        ``dateTimeSecondsSince[1980]``

    DMG
        An acronym for the Data Mining Group, an independent, vendor
        led consortium that develops data mining standards, such as
        PMML.  The group's web page is `<http://www.dmg.org>`_.

    GLR
        Generalized Likelihood Ratio.  This is one option for the test
        statistic in the :term:`Baseline Model`.
        The definition Augustus uses is the
        one given by Basseville and Nikiforov [Basseville-1993]_;
        it is a test statistic that compares the set of events in a
        moving window to a known distribution.  Currently the user
        specifies the number of events in the moving window, and the
        properties of a baseline Gaussian distribution.

    NAB
        Num Array Binary is a format used by Augustus and Augustus's
        UniTable to speed up file reading.  The file contains a text
        header with the name and format of each column of data in
        the UniTable data structure; the remainder of the file is the
        data structure itself, saved in binary format. It speeds up
        re-reading of files by around two orders of magnitude.

        To convert a CSV file to NAB format when in the same directory
        as the file, start a Python session, and then type the following::

            >>> from augustus.kernel.unitable import UniTable
            >>> tbl = UniTable()
            >>> tbl.fromfile('data_file.csv')
            ... method output omitted
            >>> tbl.to_csv_file('tab_delimited_data_file.csv')
            >>> tbl.to_nab_file('data_file_converted.nab')
            >>> exit() 

    OPTYPE
        This is a list of allowable optypes defined in PMML:
        ``categorical``, ``ordinal``, ``continuous``

    PMML
        The Predictive Model Markup Language, developed
        by the Data Mining Group (:term:`DMG`).  It is an
        :term:`XML`-based language that provides a standard way to
        communicate the contents of a data file, plus the statistical
        model that is used to analyze the data in that file.
        The benefit of a standard is that developers can create,
        test, and refine a statistical model using one
        PMML-compliant application, and then deploy it using
        another PMML-compliant application without having to write
        any custom code to translate between the two applications.

        The PMML language is described by a standard, which is
        available in the form of an :term:`XML Schema` for those
        who are interested in developing PMML-compliant applications.
        (Version 4.0 is can be found at:
        `<http://www.dmg.org/v4-0-1/pmml-4-0.xsd>`_ and is included
        in the Augustus distribution.)
        Those who just want to use Augustus will find enough
        information in the comments of the
        example PMML file :download:`example_model.pmml`. 

    PMML Consumer
        A program or application that takes two inputs: a data set,
        and a statistical model for the data, described in a PMML file.
        The PMML Consumer then outputs scores for the data set based
        on the instructions in the PMML file.  A PMML file has the
        extension :file:`.pmml`.  Augustus can be run as a PMML Consumer.

    PMML Producer
        Augustus's PMML Producers require a configuration file, a PMML
        Skeleton File, and a set of training data.  Producers can use the
        training data to automate addition of Segments to a Mining Model,
        and also to identify the properties of the distribution
        identified for each segment. (For example, the mean for a
        Poisson distribution, or the mean and variance for a Gaussian 
        distribution.)  Augustus can be run as a PMML Producer.

    PMML Skeleton
        A PMML skeleton is an incomplete PMML file that is used for
        input into Augustus's PMML producer.  The skeleton file's
        Data Dictionary and Transformation Dictionary should be complete.
        In the section where the model definition should be, the
        skeleton file should name the type of model to be used and
        list the fields to be used in that model. The configuration
        file for Augustus's PMML producer will identify all of the segments
        to place in the model, and the type of test distribution
        and test statistic to use for scoring.  
        The output of an Augustus PMML producer is a
        complete PMML file.  An example PMML skeleton and producer
        configuration file are
        in :file:`{<Install-dir>}/augustus-examples/gaslog/primer`.

    Python
        Augustus is written in the Python programming language.
        The project web page is `<http://www.python.org>`_.

    R
        R is a programming language and environment for statistical
        computing and graphics.  The R project's web site
        is:`<http://www.r-project.org>`_.

    segment
        A segment is a subset of the total range of available outcomes
        for the data.  It can be thought of as a cube or slice in the
        outcome space: for example, one sensor name out of hundreds,
        or one day in the week, or a set of hours in the day.
        
        Or a combination of these: for example morning rush hour
        (from 5am to 9am, say) on weekdays at a specific traffic sensor.
        Then, other segments could be evening rush hour, and all other times,
        or some other subdivision of the possible days, times, and sensors.

    UniTable
        An Augustus Python module, a data structure, and a file format
        created
        by Open Data Group and included in the Augustus package. 
        The Python module defines the data structure and its properties.
        The data structure is analogous to a data frame in the
        :program:`R` statistical programming system: briefly, a data
        structure containing an array.  The array's columns
        can have different data types, and are accessible by an index
        number or a string label.
        It can also convert CSV files to XML or UniTable files.
        When a file format is called ``UniTable``, it refers to the
        :term:`NAB` file format.  

        See also the UniTable section of the Augustus code site:
        `<http://code.google.com/p/augustus/wiki/UniTable>`_

    XML
        An acronym for the eXtended Markup Language; a format for
        communicating information that can be structured as a
        tree.
        Examples of XML-based languages are PMML, which
        Augustus uses to describe its statistical models; and
        the HyperText Markup Language (HTML), which
        is used to describe the layout of a web page.
        The Augustus configuration files are also written using
        an XML format: one to produce a PMML model,
        and another to consume a PMML model.

    XML Schema
        A standardized way to communicate the 
        format of an :term:`XML`-based language.  PMML
        is described using the schema located at
        `<http://www.w3.org/2001/XMLSchema>`_;
        developers who wish to contribute to modifications
        of future PMML standards would need to understand
        the schema; those who just want to use Augustus
        need not go to this level of detail.

    Z-Value
        The Z-Value test is a change detection test that measures
        the how likely an observation is to occur in relation to
        a reference distribution.
        More details are available on wikipedia at
        `<http://en.wikipedia.org/wiki/Standard_score>`_.

.. TANYA -- This page DONE.
