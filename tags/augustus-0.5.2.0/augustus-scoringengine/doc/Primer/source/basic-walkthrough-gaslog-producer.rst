.. raw:: latex


    \newpage


Walkthrough: gaslog producer
============================

The :file:`augustus-examples/aim/gaslog` directory demonstrates how to use
Augustus as both a PMML model producer and consumer.  This small problem
demonstrates using a baseline model to calculate the z-score of new
observations relative to a known distribution, and then demonstrates the use of
variants on the baseline model outputs and configurations.

Steps
-----

        1. Identify data.
        #. Organize the project.
        #. Preprocess the data, if necessary.
        #. Create a PMML file that describes the model.
        #. Run Augustus.
        #. Post-process the results.

.. _gaslog_identify_data:

Identify suitable data
----------------------

The data are from an Augustus contribor, Jim, who kept a log of every time he
filled his car with gas since midway through 2003.  The data file,
:file:`augustus-examples/gaslog/gaslog.xml`, is XML-formatted; Augustus can
other file types such as CSV.

His gas log contains the date, gallons, car mileage, miles traveled since the
previous fill-up, the price he paid in dollars, and whether the car was an
old one or a new one.  There are missing data fields sometimes; Augustus handles
this by reporting a score of `Missing` if a variable is missing or `Invalid` if
a variable is derived from a missing value or the derivation involves a division
by zero or a result that cannot be cast to the derived data type.

In this example, the goal is to show how two common change-detection models are
expressed in PMML, and how to run Augustus. Additional advanced examples
demonstrate other tools Augustus provides to create and modify models.

.. _gaslog_create_folder:

Create a project directory (organize your project)
--------------------------------------------------

No particular project directory structure is required by Augustus; all input
and output file names and paths are specified in the XML configuration files.
The directory for this project is in :file:`augustus-examples/gaslog/`, which
has the following subdirectories::

    gaslog/
      data/
      advanced/
      introductory/

Data files are stored in :file:`gaslog/data`, and are shared between two
projects. This primer describes the contents of the :file:`introductory`
directory; the :file:`advanced` directory contains a slightly more advanced use
case of Augustus, with instructions in a :file:`README` file.  Makefiles for
each step are provided for the advanced example.

A typical project file structure would separate the data from the consumer
model schema (for example, :file:`example_model.pmml`) and  configuration file
(for example, :file:`consumer_config.xcfg`).  This makes it easier to manage
multiple data files, configuration files, and model schema during development.
Under :file:`introductory` are the folders::

   introductory/
     config/
     models/
     results/

the configuration files in :file:`gaslog/config` are in XML format, and
communicate the location of the data, the PMML model, and other details about
how to run to Augustus.  The :file:`models` directory contains the statistical
models, described using the :term:`PMML` format.  The :file:`results` directroy
will contain output scores. Additional directories may be valuable for some
projects, in which there could be preprocessing or post processing scripts.
(This release of Augustus provides new tools for common post processing steps.)

.. _gaslog_prepare_data:

Pre-processing (Prepare the data)
---------------------------------
In this example, the data were manually entered into an XML format in
:file:`gaslog/data/gaslog.xml`, and need no further processing.

In general, the input data have to be readable by Augustus.  Augustus's PMML
consumer can analyze data in an :term:`XML` format (possibly output from a
database), from a CSV file, or in its own :term:`UniTable` format (with file
extension :file:`.nab`).

The UniTable is a data structure analogous to a data frame in the :term:`R`
statistical programming system: briefly, a data structure containing an array
with columns that can have different data types, and whose columns are also
accessible by a string label.

UniTable also has methods to read directly from a CSV or otherwise delimited
file, and can deduce the delimiter.  Below are the first and last few lines
of :file:`gaslog/data/gaslog.xml`.  The entries in each row are accessible by
their tag names, for example ``date`` or ``mileage``.  The tag names `table`
and `row` can be any XML-compliant string the user chooses::

   <table>
     <row>
       <date>2003/06/02</date> <gallons>14.7</gallons> <car>old</car>
     </row>

     ...

     <row>
       <date>2011/09/22</date>
       <gallons>10.043</gallons>
       <mileage>60882</mileage>
       <miles>334.0</miles>
       <price>38.86</price>
       <car>new</car>
     </row>
   </table>

.. _gaslog_producer_provide_model:

Provide a model schema
----------------------

The model can be built once every field in the data set has been named
(e.g. ``date`` and ``mileage``) and the data types of each field are known
(``date`` is an ISO 8601 formatted date string; the same date format used in
HTML; and ``mileage`` is an integer).

The model is written in :term:`PMML`, a XML-based language that describes
predictive models.  First, it defines the format of the data, and then
(optionally) it defines the model that will be applied to the data.

Augustus can calculate the properties of a distribution when given a set of
training data, and can automate the model segmentation, it just has to be told
where the training data are, where the model skeleton is, and where to
put the trained model.  An example PMML skeleton is in
:file:`{<Install-dir>}/augustus-examples/gaslog/introductory/models/model_template.pmml`.

As a brief interlude:  XML elements in a PMML file have to be present in a
specific order. Their placement, and whether or not they are required, is
determined by the standards body (the Data Mining Group), and communicated in
PMML's XSD schema, available
at: `<http://dmg.org/v4-1/GeneralStructure.html>`_.  A user who wants access
to a richer set of controls than are presented here will have to learn to read
through pieces of the DMG's schema in order to find out where to put parts of
the model.  ODG will also soon offer a graphical tool to help with model
creation--check back on our web site for this.

The best way to start describing a statistical model in PMML format is to copy
and paste from existing files and change, delete, or add things to meet the
current model's needs.  The file :file:`example_model.pmml`  is commented to
identify some of the commonly available options for a Baseline Model.
It is also included in the :ref:`Example section <example_pmml_baseline>`.
The sequence of elements in a PMML file is fixed; items must appear in the order
specified. The smallest possible complete PMML file for this example looks
like::

  <PMML version="4.1">
      <Header />
      <DataDictionary>
          <DataField name="miles" dataType="double" optype="continuous" />
      </DataDictionary>
  </PMML>

In the above, all optional elements, including the specification
of a statistical model, are omitted.  At least one data field
is required to be defined in the data dictionary.  The
field name should be the same name as the column name
in a UniTable, or the element name in XML-formatted information
like our example of ``gallons`` and ``miles``.
The attributes for the data field depend on the nature of the data;
``optype`` can be  ``categorical``, ``ordinal`` or ``continuous``.

After the data dictionary comes an optional transformation dictionary.  The
transformation dictionary defines functions that can be applied to the data,
and any derived fields that could be used in the subsequent model.  Finally
comes the optional model specification.  With these elements, the document would
look something like the below::

  <PMML version="4.1">
      <Header copyright="Open Data Group, 2011" />
      <DataDictionary>
          <DataField name="date" optype="continuous" dataType="date" />
          <DataField name="gallons" optype="continuous" dataType="double" />
            .
            . (Insert additional data fields here...)
            .
      </DataDictionary>
      <TransformationDictionary> 
            .
            . (With optional DefineFunction elements followed
            .  by optional DerivedField elements.)
            .
      </TransformationDictionary>
      <MiningModel functionName="regression">
            .
            . (Or another type of model; Augustus has algorithms
            .  to support MiningModel, BaselineModel, ClusteringModel, etc...
            .  The contents of the MiningModel are described 
            .  separately below.)
            .
      </MiningModel>
  </PMML>

Since options are listed in the comments in :file:`example_model.pmml`, they
are not described in this tutorial.  The mining model has an
option for segmentation, that makes it possible to score only a
subset of the data.  In this example, the segment template will
never be calculated because its predicate is ``False``; it will evaluate
to False when compared against any data.  The other segments
are not yet generated, but there will be 24 segments created;
one for each of Jim's two new cars multiplied by the twelve
months of the year.

The smallest ``MiningModel`` element in a PMML file would look something like
this::

      <MiningModel functionName="regression">
          <MiningSchema>
              <MiningField name="date" />
          </MiningSchema>
      </MiningModel>


The mining schema would list every field to be used in the
statistical model; the ``name`` attribute must be the name
of one of the ``DataField`` elements in the data dictionary,
or the name of a ``DerivedField`` element in the transformation
dictionary.  Not all defined data fields or derived fields need
to be used.  Again, options are not described in this tutorial
but are in the comments of the :ref:`Example <example_pmml_baseline>`
section. With segments, the mining model would look more like::

      <MiningModel functionName="regression">
          <MiningSchema>
              <MiningField name="date" />
              .
              . (Additional items omitted for brevity)
              .
          </MiningSchema>
          <Segmentation multipleModelMethod="selectAll">
              <Segment>
                  <SimplePredicate field="car" operator="equal" value="old" />
                  <BaselineModel functionName="baseline">
                  .
                  . (A simple or compound predicate determines which
                  .  data belong to which segment.  Then the model
                  .  is inserted.  The details of the model are only
                  .  in the actual example file, for brevity.)
                  . 
                  </BaselineModel>
              </Segment>
              .
              . (As many segments as needed...)
              .
          </Segmentation>
      </MiningModel>


To make complex models, it will eventually be necessary to read the PMML schema
and find the elements and attributes used to describe parts of that model. All
existing schema are available from the
DMG web site: `<http://www.dmg.org/>`_ [#f1]_.

.. _gaslog_run_producer:

Run Augustus as a model producer
--------------------------------

Running Augustus as a `model producer` means either training an existing PMML
model file or creating segments to populate a PMML model given a skeleton file.

The first reason to use Augustus' producer tools is to identify baseline
statistics for a model from existing data during a period of normal operation.
The second reason is to automatically create PMML segments in a model.
Augustus typically handles thousands or tens of thousands of segments; the
actual upper bound depends on computer memory. Even though a person could
manually edit a model with 24 segments, even small numbers of segments make
editing at best tedious and at worst error-prone.

One reason to create segmented models is to capture consistent behavior that is
different on different days, for example the amount of traffic on the roads on
weekends versus weekdays, or during rush hour versus during the early morning.
Then, all of the traffic on these different days and times can be grouped
together, and the baseline statistics related to each conceptual group can be
calculated separately to provide a richer insight into the overall system
behavior and more accurate inferences about when observed behavior does not
match the expected behavior.

Create a PMML skeleton file
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The skeleton file is a PMML document containing:

   * A complete header section.
   * A complete data dictionary.
   * A complete transformation dictionary (if derived fields are used).
   * The shell of a model, which
      - *Must* contain the MiningSchema section that lists
        fields used in the segments or for calculation.
        (Or it can contain another model type, but then it would not
        produce any segments, just train the existing model with new data.)
      - *Can* contain an Output section if there are OutputFields that are
        common to all segments that the user wants to include with the calculated
        scores.
      - *Can* contain a LocalTransformations section that describes the
        common to all segments that the user wants to include with the calculated
        scores.
      - *Must* contain the Segmentation section that will
        eventually contain segments, and may contain some segments already.
      - *Must* contain a special Segment, with ``id="ODG-SegmentTemplate"``
        and predicate ``<False />`` as the selection predicate. This template
        will be used to auto-generate all of the segments requested in
        the Augustus configuration file.  It should contain a complete
        model exactly the way you want it to be, except with zeros or nonsense
        data for the description of the distribution; this will be replaced
        when Augustus trains the model.

The model template that will be described in this primer is

:file:`{<Install-dir>}/augustus-examples/gaslog/introductory/models/model_template.pmml`.
The LocalTransformations element is described here to introduce how to define
DerivedFields and how to apply functions.  There are three transformations: the
first converts the ``date`` from an ISO date string, which is the date format
used for date strings in PMML, to the year, and names the new variable ``year``.
The second converts ``date`` to the month, and the third estimates the miles per
gallon by dividing the miles between fill-ups by the gallons in the current
fill-up.  The PMML excerpt that defines the new field ``mpg`` is::

    <LocalTransformations>
        <DerivedField name="mpg"
                      optype="continuous"
                      dataType="double">
            <Apply function="/">
                <FieldRef field="miles" />
                <FieldRef field="gallons" />
            </Apply>
        </DerivedField>
        .
        . (With additional DerivedField elements below...)
        .
    </LocalTransformations>

in which each derived field is named and identified. The
:samp:`<Apply function="{a_function}">  ... </Apply>` tags describe a function
call in PMML.  Arguments to the function are passed in the order they are
written, so the above PMML could be translated to something like::

   mpg = miles / gallons
   
Some predefined functions are ``+``, ``-``, ``*``, ``log10``, ``ln``, ``sqrt``,
``abs``, ``exp``, ``round``, ``uppercase``. The model template also
contains the function ``formatDateTime``, which is very useful for formatting
temporal data.  It uses conversion strings that are exactly the same as that
used by C's strftime on (on UNIX-like systems, type ``man strftime`` at a
command prompt for a description). These and a few dozen others are described
on the DMG web site:
`<http://www.dmg.org/v4-1/BuiltinFunctions.html>`_

Derived fields can be used in exactly the same way as defined fields; they can
be referred to in output, for scoring, and can be used in any derived field
defined below its definition.

For the type of model described here, the most important part of the PMML file
is the definition of the Segment Template.  The template in
:file:`model_template.pmml` is excerpted here::

    <Segment id="ODG-SegmentTemplate" >
        <False/>

        <BaselineModel functionName="regression">
            <MiningSchema>
                <MiningField name="mpg" />
                <MiningField name="score" usageType="predicted" />
            </MiningSchema>

            <Output>
                <OutputField name="score" feature="predictedValue" />
            </Output>

            <TestDistributions field="mpg" testStatistic="zValue">
                <Baseline>
                    <GaussianDistribution mean="0." variance="1." />
                </Baseline>
            </TestDistributions>
        </BaselineModel>
    </Segment>

It will be copied exactly and inserted into the output PMML file
for every new segment observed, but with the ``GaussianDistribution``'s
properties changed to have a mean and a variance that matches the mean and
variance in the training data.  Also, the segment id will be an automatically
generated number, and most important, predicate ``<False />`` will be replaced
by a predicate that selects data for a specific segment.  Example predicates
are::

   <SimplePredicate field="year" operator="equal" value="2005" />

or::

   <SimplePredicate field="year" operator="notequal" value="2003" />

or::

   <CompoundPredicate booleanOperator="and">
      <SimplePredicate field="year" operator="equal" value="2005" />
      <SimplePredicate field"car" operator="equal" value="old" />
   </CompoundPredicate>


During training, each time that a data point is processed which does not fall
into any of the existing segments, Augustus will check to see whether any of
the user's requested Segment predicate specifications match, and if so will
create an appropriate segment.


Augustus's configuration file for Producing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Augustus Producer configuration file specifies, in the following order:

   1. Whether to log errors and information, the format of the logging
      string, and where to put logging information messages.
   #. The location of the PMML model skeleton.
   #. The location of the input data.
   #. All information related to creating the model:

      a. The location of the output model file
      #. The minimum acceptable number of data points to define a data
         set before model will return a valid score. [Default is one.]
      #. How segments are defined.

The best way to understand
the options available for using Augustus as a PMML file producer
is to modify the configuration file and run it multiple times. A commented
version of :file:`producer_config.xcfg` is available in the
:ref:`Example section <example_baseline_producer_config>`.

.. _gaslog_producer_define_segments:

Define the segments
^^^^^^^^^^^^^^^^^^^

Part of building the model is choosing how to segment the data for
scoring. The MiningModel in :file:`model_template.pmml` lists data with six dimensions::

          <MiningSchema>
            <MiningField name="date" />
            <MiningField name="gallons" />
            <MiningField name="mileage" />
            <MiningField name="miles" />
            <MiningField name="price" />
            <MiningField name="car" />
          </MiningSchema>

The miles per gallon calculated from the gallons and miles fields is
the variable of interest; the purpose of all of the other dimensions is
either for supplementary information to be used in the output, or to
assign the miles per gallon to a specific segment.

.. note::

    If a field is not identified in the MiningField of the MiningModel's MiningSchema
    or derived in LocalTransformations after that MiningSchema, then it
    cannot be used in Segmentation predicates, Output, or in calculating the
    score.

The producer
configuration file :file:`producer_config.xml` divides the data by car and month,
meaning data related to the same car, that are collected in the same month of
the year, belong to the same Segment of the data set, and will contribute to
the same Baseline probability distribution.
Both car and month are implemented as *enumerated dimensions*; every
individual value in the category will belong to a different Segment in
the PMML model.

.. _gaslog_producer_run_producer:

Build the PMML
--------------

In this section, multiple input configurations will be run to clarify how
different segmentation options work, and what kind of error messages occur.
To run Augustus with the example configuration file, change directories into
:file:`{<Install-dir>}/augustus-examples/gaslog/introductory/config`.  Then type::

   $ Augustus producer_config.xml

Output should begin like this::

    root     : INFO     Loading PMML model.
    root     : INFO     Setting up data input.
    root     : INFO     Setting up model updating/producing.
    root     : INFO     Setting up Augustus's main engine.
    root     : INFO     Calculating.

and should continue to completion, stating when it sees new segments::

    META INFO   New segment created: (car EQ 'old') and (month EQ 'Jun'), ID=Untitled-1
    root     : WARNING  Data not found for field: mileage
    root     : WARNING  Data not found for field: miles

The information about new segments is communicated through
the Metadata logger, and information about what Augustus
is doing and general problems are communicated through the
root logger (set up using Logging).  The warning messages
indicate that there are missing columns in some of the entries
in the data set. The verbosity of the messages can be changed
by setting the Logging and Metadata logging levels to higher
values (like 'WARNING' or 'ERROR') or by removing these elements
from the configuration file.

The output continues::

    META INFO     New segment created: (car EQ 'new') and (month EQ 'Mar'), ID=Untitled-23
    META INFO     New segment created: (car EQ 'new') and (month EQ 'Apr'), ID=Untitled-24

and then, a summary of the run is printed::

    META INFO     ### Current MetaData content ###
    META INFO     Run time : 0.322700977325
    META INFO     Score calculation, total : 0.0364532470703
    META INFO     Time Reading Data : 0.0998890399933
    META INFO     Time searching for blacklisted items : 0.00146222114563
    META INFO     Time to advance through data : 0.0380392074585
    META INFO     Time to find and create new PMML segments : 0.0507431030273
    META INFO     Time to load PMML model : 0.0140819549561
    META INFO     Time to look up existing segments : 0.0162174701691
    root     : INFO     Augustus is finished.

A full model should now be in

:file:`{<Install-dir>}/augustus-examples/gaslog/introductory/models/produced_model.pmml`.

The following subsections demonstrate some additional configuration options.
Since we will be modifying the producer configuration file, it would be a good
idea to copy :file:`producer_config.xcfg` to another file name, say
:file:`test_config.xcfg` so that the original file does not get lost.


.. _gaslog_producer_segmentation_options:

Segmentation options
^^^^^^^^^^^^^^^^^^^^

Augustus can handle thousands of segments, but even 24 make the PMML model
file difficult to present in documentation.  To reduce the size for a better
discussion and to learn the format of the configuration file, open
:file:`test_config.xcfg` and modify the ``EnumeratedDimension`` element with
``field="month"`` to specifically list only January and February.  The entire
file should now look like the below::

    <AugustusConfiguration>
        <Logging
            formatString="%(name)-9s: %(levelname)-8s %(message)s" level="INFO">
            <ToStandardError />
        </Logging>

        <Metadata
            formatString="META %(levelname)-8s %(message)s" level="INFO">
            <ToStandardError />
        </Metadata>

        <ModelInput>
            <FromFile name="../models/model_template.pmml" />
        </ModelInput>

        <DataInput>
            <ReadOnce />
            <FromFile name="../../data/gaslog.xml" />
        </DataInput>

        <ModelSetup outputFilename="../models/produced_model.pmml"
            mode="replaceExisting"
            updateEvery="event">

            <SegmentationSchema>
                <GenericSegment>
                    <EnumeratedDimension field="car" />
                    <EnumeratedDimension field="month">
                        <Selection value="Jan" />
                        <Selection value="Feb" />
                    </EnumeratedDimension>
                </GenericSegment>
            </SegmentationSchema>
        </ModelSetup>

   </AugustusConfiguration>

except that there are commented sections in the actual file that are omitted
above.  If you run the producer with the new configuration file::

   $ Augustus test_config.xcfg

there will now be ``INFO`` messages peppering the rest of the logger
output--to tell the user that a data row (an Event) did not match any
of the segments, and would therefore be discarded (lines are wrapped for
display purposes)::

    root     : INFO     Loading PMML model.
    root     : INFO     Setting up data input.
    root     : INFO     Setting up model updating/producing.
    root     : INFO     Setting up Augustus's main engine.
    root     : INFO     Calculating.
    root     : WARNING  Data not found for field: mileage
    root     : WARNING  Data not found for field: miles
    root     : WARNING  Data not found for field: price
    root     : INFO     Event 0 did not match any segment descriptions; discarding.
                          Data=car:old, date:2003-06-02, gallons:14.7...
    .
    . (intermediate output between events 0 and 185 is omitted...)
    .
    root     : INFO     Event 185 did not match any segment descriptions; discarding.
                          Data=car:new, date:2011-08-21, gallons:9.794...
    root     : INFO     Event 186 did not match any segment descriptions; discarding.
                          Data=car:new, date:2011-09-22, gallons:10.043...
    META INFO     ### Current MetaData content ###
    META INFO     Run time : 0.267997980118
    META INFO     Score calculation, total : 0.0104904174805
    META INFO     Time Reading Data : 0.0434489250183
    META INFO     Time searching for blacklisted items : 0.00136375427246
    META INFO     Time to advance through data : 0.037558555603
    META INFO     Time to find and create new PMML segments : 0.0111610889435
    META INFO     Time to load PMML model : 0.0145878791809
    META INFO     Time to look up existing segments : 0.0132262706757
    root     : INFO     Augustus is finished.

if the ``INFO`` messages are too frequent, change the level in the
``<Logging />`` element to ``WARNING`` or ``ERROR``.  With it set at ``ERROR``
there is no main logging output, only  Metadata output, and the test run will
look like::

    META INFO     New segment created: (car EQ 'old') and (month EQ 'Jan'), ID=Untitled-1
    META INFO     New segment created: (car EQ 'old') and (month EQ 'Feb'), ID=Untitled-2
    META INFO     New segment created: (car EQ 'new') and (month EQ 'Jan'), ID=Untitled-3
    META INFO     New segment created: (car EQ 'new') and (month EQ 'Feb'), ID=Untitled-4
    META INFO     ### Current MetaData content ###
    META INFO     Run time : 0.204999923706
    META INFO     Score calculation, total : 0.0100080966949
    META INFO     Time Reading Data : 0.0791020393372
    META INFO     Time searching for blacklisted items : 0.00152134895325
    META INFO     Time to advance through data : 0.0427370071411
    META INFO     Time to find and create new PMML segments : 0.0120093822479
    META INFO     Time to load PMML model : 0.0135910511017
    META INFO     Time to look up existing segments : 0.0136258602142

The only segments created were for January and February, for the two different
types of cars.  We can confirm that only four segments exist by looking in the
full PMML file: :file:`models/produced_model.pmml`

Suppose we want to partition the months into groups rather than into categories.
There is also an option for a *Partitioned Dimension* that can be commented out
and used in place of the enumerated months.  In the example, the months of the
year (ranging from 1 to 12) are divided into five segments.  This could, for
example, capture that Jim logged more highway driving (and presumably better gas
mileage) during months with traditional holidays, like June through August and
November and December.  We will comment out the previous separately from months
that usually contain work driving. We can modify :file:`test_config.xcfg`
further to define a Partitioned Dimension.  Below is an excerpt of the
Segmentation Schema only::

    <SegmentationSchema>
        <GenericSegment>
            <EnumeratedDimension field="car" />
            <PartitionedDimension field="intmonth">
                <Partition low="0" high="5" />
                <Partition low="6" high="8" closure="closedClosed" />
                <Partition low="8" high="12" divisions="2" closure="openClosed" />
            </PartitionedDimension>
        </GenericSegment>
    </SegmentationSchema>

The example shows that there can be more than one partition in
a ``PartitionedDimension`` element, making it possible to segment
the data using nonuniform ranges.

By default, the range for each bin is open on the low end and
closed on the high end.  The default number of divisions is 1.
Allowed values for the ``closure`` attribute are
``"closedOpen"`` and ``"openClosed"`` when there are more than
one division.  If there is only one division, ``closure`` can
also be ``"closedClosed"`` and ``"openOpen"``.
The ranges defined above are:
(0, 5], [6, 8], (8, 10], and (10, 12]. The output will look like (lines are
wrapped for display purposes)::

   Augustus test_config.xcfg

   META INFO     New segment created: (car EQ 'old') and ((intmonth GE 6) and
                      (intmonth LE 8)), ID=Untitled-1
   META INFO     New segment created: (car EQ 'old') and ((intmonth GT 8) and
                      (intmonth LE 10)), ID=Untitled-2
   META INFO     New segment created: (car EQ 'old') and ((intmonth GT 10) and
                      (intmonth LE 12)), ID=Untitled-3
   META INFO     New segment created: (car EQ 'old') and ((intmonth GT 0) and
                      (intmonth LE 5)), ID=Untitled-4
   META INFO     New segment created: (car EQ 'new') and ((intmonth GT 0) and
                      (intmonth LE 5)), ID=Untitled-5
   META INFO     New segment created: (car EQ 'new') and ((intmonth GE 6) and
                      (intmonth LE 8)), ID=Untitled-6
   META INFO     New segment created: (car EQ 'new') and ((intmonth GT 8) and
                      (intmonth LE 10)), ID=Untitled-7 
   META INFO     New segment created: (car EQ 'new') and ((intmonth GT 10) and
                      (intmonth LE 12)), ID=Untitled-8
   META INFO     ### Current MetaData content ###
   META INFO     Run time : 0.247933149338
   META INFO     Score calculation, total : 0.0319168567657
   META INFO     Time Reading Data : 0.0750558376312
   META INFO     Time searching for blacklisted items : 0.00143218040466
   META INFO     Time to advance through data : 0.0404446125031
   META INFO     Time to find and create new PMML segments : 0.0173301696777
   META INFO     Time to load PMML model : 0.0142331123352
   META INFO     Time to look up existing segments : 0.0179903507233

.. _gaslog_producer_blacklisting:

Blacklisting segments
^^^^^^^^^^^^^^^^^^^^^

If we check in the new model :file:`gaslog/introductory/models/produced_model.pmml`,
some of the baseline Gaussian Distributions for the miles per gallon
look suspect.  From within :file:`gaslog/introductory/config/`, type::

   $ grep "mean" ../models/produced_model.pmml

the output should look like::

    <GaussianDistribution variance="1.0" mean="0.0" />
    <GaussianDistribution variance="1106.06928831" mean="41.9758043943">
    <GaussianDistribution variance="58.870609039" mean="28.7080709112">
    <GaussianDistribution variance="94.7170020643" mean="29.4167089563">
    <GaussianDistribution variance="80.1025060911" mean="27.7009284687">
    <GaussianDistribution variance="7015428.31403" mean="-554.894869158">
    <GaussianDistribution variance="1302.66492634" mean="46.2428828612">
    <GaussianDistribution variance="29692.5897065" mean="36.0739058162">
    <GaussianDistribution variance="1020.96454195" mean="49.6721277817">

The first mean and variance, with ``variance="1.0"`` and
``mean=0.0"`` are from the segment template,
and the rest are from the different month and car combinations.
Some of them have a much bigger variance, and one has a negative
mean.  This is partly from typos during Jim's data entry and partly from missing
receipts, and is a good example of the way real data look. Suppose, just for
the sake of example, that a good option would be to ignore data
points with negative total miles or in which the total miles between
fill-ups are clearly too large to have been from one single gas tank.

One alternative is to use the ``<BlacklistedSegments />``
configuration option.  Every record is compared with the entries
in the ``BlacklistedSegments`` and if it matches, the record will
be ignored. This means, if you are producing a model, the record
will not contribute to the model's training. When the
``<BlacklistedSegments />`` option exists and Augustus is
consuming a model, the score output will still be logged,
but it will be empty, like this, ::

   <Event id="10"></Event>

even if another segment in the model would have matched. To black list an item,
first, make sure that the field with blacklisted contents exists in the
LocalTransformations or MiningSchema section of the PMML Mining Model, or else
Augustus will not be able to locate the value being used.  In our case,
the `mpg` field name is defined in the LocalTransformations, so lookup will
not be a problem.  Next, add a ``BlacklistedSegments`` section to the configuration
file.  It goes right before the ``GenericSegment`` section::

    <ModelSetup outputFilename="../models/produced_model.pmml"
                mode="replaceExisting" updateEvery="event">
        <SegmentationSchema>

            <BlacklistedSegments>
                <PartitionedDimension field="mpg">
                    <Partition high="5" closure="openClosed" />
                    <Partition low="100" closure="closedOpen" />
                </PartitionedDimension>
            </BlacklistedSegments>

            <GenericSegment>
              .
              . (entries omitted for brevity)
              .
            </GenericSegment>
        </SegmentationSchema>
    </ModelSetup>

The above will blacklist the following ranges (-infinity, 5] and
[100, infinity); to mask the extreme entries.  Run Augustus again with the
modified inputs::

   $ Augustus test_config.xcfg

and the new output will now have more reasonable means::

   $ grep "mean" ../models/produced_model.pmml

    <GaussianDistribution variance="1.0" mean="0.0" />
    <GaussianDistribution variance="716.528207605" mean="48.1312898116">
    <GaussianDistribution variance="58.870609039" mean="28.7080709112">
    <GaussianDistribution variance="94.7170020643" mean="29.4167089563">
    <GaussianDistribution variance="80.1025060911" mean="27.7009284687">
    <GaussianDistribution variance="346.793653314" mean="45.184295448">
    <GaussianDistribution variance="13.2770564116" mean="32.7013033238">
    <GaussianDistribution variance="166.84629477" mean="38.4476267558">
    <GaussianDistribution variance="177.798362894" mean="41.0303215955">


.. _gaslog_producer_run_consumer:

Run the Augustus consumer
-------------------------

Statistically, it is not appropriate to run this model against the same data we
used to train it, but we can still do it for the sake of the walk through.
Augustus can be run from command-line mode if the user does not need any
logging, segmentation, or model updating.  To do this, type::

   $ Augustus --model ../models/produced_model.pmml \
      --data ../../data/gaslog.xml  > ../results/output.xml

This output will not have an opening and closing tag, so if you use it as an
input to other programs, remember to wrap it. There should be no output to the
screen, but if you change directories to :file:`../results/` you should see the
:file:`output.xml` file.


.. _gaslog_producer_postprocess:

Post-process the results
------------------------

In this part of the example, there is no post-processing; presumably the output
will be sent to another program that will then send alert notifications or
update a display... so congratulations, you have successfully run of Augustus as
a Baseline Producer to create a PMML-formated segmented model from a set of
training data.

.. rubric:: Footnotes

.. [#f1]
         Open Data Group is a member of the Data Mining Group; the group that
         manages the PMML standard.
