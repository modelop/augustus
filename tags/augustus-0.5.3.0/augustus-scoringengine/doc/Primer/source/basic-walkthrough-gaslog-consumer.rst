.. raw:: latex

    \newpage

.. _gaslog_consumer_tutorial:

Walkthrough: gaslog consumer
============================

To present the consumer, a sample model is provide if you do not want to work
through the steps required to produce a model above.

In this section, the The data set is the same as with producing, so the initial
steps of data identification, project organization, preprocessing, and model
creation have been done. The purpose of this section is to focus on how to
consume PMML models using Augustus.

.. _gaslog_provide_schema:

Provide a model schema
----------------------

The model can be built once every field in the data set has been named
(e.g. ``date`` and ``mileage``) and the data types of each field are known
(``date`` is an ISO 8601 formatted date string; the same date format used in
HTML; and ``mileage`` is an integer).

The goal in this example is to score the values from the :file:`gaslog.xml`
data set against the distributions in the example model file
:file:`gaslog/introductory/models/example_model.pmml`.  The two test distributions
chosen are :program:`Chi squared independence` and :program:`z-value` tests.

The z-value test is described first: for every row of data Augustus will
score, it compares the new observed data point to a known distribution and
calculates the z-value; the number of standard deviations the observed value is
away from the mean. Augustus can score against a Gaussian or a Poisson
distribution.

The Chi squared test does remember events.  That is, for every row of data
Augustus will score, it compares the distribution currently in memory with the
model distribution and calculates the current score. The score indicates the
probability that the observed distribution is different from the model
distribution, so a value of one means the observed data are orthogonal to the
model data, and a value of zero means the observed data exactly match the
model data. More details about the meaning of the score are available at the
DMG web site: `<http://www.dmg.org/>`_.

.. _gaslog_run_consumer:

Run Augustus as a model consumer
--------------------------------

The Augustus consumer, like the producer, requires configuration with a
XML-formatted file.  The consumer configuration file tells the consumer
where the model is, where the data are, and where to write the output.

Just like with the producer configuration, the best way to get started is just
to open the configuration file and copy and modify if for your needs.  An example
file is included in the :ref:`Example section <example_consumer_config>`.  If you
would like to get a sense for the output of the Augustus PMML consumer, you can
run the consumer (scoring engine) using the example consumer config file. Just
copy it into your :file:`{<Install-dir>}/augustus-examples/gaslog/consumer`
directory and then type the following at the command prompt::

    $ Augustus consumer_config.xcfg

The output should look like this::

    root     : INFO     Loading PMML model.
    root     : INFO     Setting up data input.
    root     : INFO     Setting up output.
    root     : INFO     Setting up Augustus's main engine.
    root     : INFO     Calculating.
    root     : WARNING  Data not found for field: price
       .
       . (additional warning messages about missing data omitted)
       .
    root     : WARNING  Data not found for field: gallons
    root     : WARNING  Data not found for field: date
    root     : INFO     Augustus is finished.


and a new file, :file:`example_scores.xml` should now be in your

:file:`{<Install-dir>}/augustus-examples/gaslog/introductory/results/`
directory with the output from the PMML consumer.

If you open the configuration file, you will notice that the path to the model
file is specified as::

       <ModelInput>
         <FromFile name="../models/example_model.pmml" />
       </ModelInput>

If instead you changed directories to
:file:`{<Install-dir>}/augustus-examples/gaslog` and tried to type the
following::

   $ Augustus consumer/consumer_config.xcfg

You would see an error message::

    root     : INFO     Loading PMML model.
    Traceback (most recent call last):
      File "/home/odg/augustus-dum/augustus-scoringengine/augustus/bin/Augustus", 
        line 49, in <module>
          main(options.config)
      File "/home/odg/augustus-dum/augustus-scoringengine/augustus/engine/mainloop.py",
        line 265, in main
          pmmlModel, pmmlFileName = getModel(child)
      File "/home/odg/augustus-dum/augustus-scoringengine/augustus/engine/mainloop.py",
        line 111, in getModel
          raise RuntimeError, "no files matched the given filename/glob: %s" % filename
     RuntimeError: no files matched the given filename/glob: ../models/example_model.pmml

The error message is long.  Look at the last line to see the actual message. The
rest is just traceback information for a programmer. ::

     RuntimeError: no files matched the given filename/glob: ../models/example_model.pmml

The message means Augustus could not find the model file because we ran Augustus in the
directory :file:`{<Install-dir>}/augustus-examples/gaslog` and if, from this directory
we followed the path :file:`../models/example_model.pmml` we would go up one directory
and look for the :file:`models` folder, which does not exist.  The solution is to either
change the path to the model to become, and then run Augustus from this directory::

       <ModelInput>
         <FromFile name="models/example_model.pmml" />
       </ModelInput>

or to change directories back to
:file:`{<Install-dir>}/augustus-examples/gaslog/consumer` and run Augustus
from that directory.  The point is just to make sure that all of the paths point
to the right place...and to introduce you to how Augustus communicates when
there are errors.

.. note::

    The PMML consumer will append to, not overwrite, an output
    file by default.  Use the attribute `overwrite="true"` to
    overwrite any existing output.

There should now be a results file,

:file:`{<Install-dir>}/augustus-examples/gaslog/results/example_scores.xml`.
Currently Augustus only creates XML-formatted output, but in the future
it will support additional formats such as JSON.

Because it can be confusing exactly what the score means for a Chi Squared Distribution,
we intentionally trained the model with data from the first half of the data set--through
the end of 2006.  The predicted value for a Chi Squared Distribution is one if the
distribution in the scoring data is orthogonal to the distribution in the model, and zero
if the distribution in the scoring data matches the distribution in the model.
Events for the dates 2004-01-03 through 2004-11-28 all have a score of one.  The
score decreases as the observed data start to match the training data, and becomes
zero on 2006-12-02, when the PMML model's training data set matches the current amount of
information available to Augustus.  After that, the score increases as the collected
information in Augustus diverges from the PMML model's training set.


.. _gaslog_postprocess:

Post-processing the results
---------------------------

This section gives an example post-processing step that turns the XML
output into a more human-readable CSV format.

With Augustus comes a handful of tools to manipulate PMML model files
(this becomes useful when a model has a few thousand segments, to spare
the user the tedium of changing things by hand) and to manipulate the output.
They are all in the file
:file:`{<Install-dir>}/augustus-scoringengine/augustus/bin`.
The one described here is :program:`ScoresAwk`; named
after `AWK <http://en.wikipedia.org/wiki/AWK>`_
because its function, structure, and command names are similar.
It is not necessary to understand any AWK to continue with the example.  Some of
the other tools are :program:`PmmlSplit` and :program:`PmmlSed`.

Change directories into the folder:
:file:`{<Install-dir>}/augustus-examples/gaslog/postprocess`.
It contains another configuration file, that will be fed to the
:program:`ScoresAwk` script.  It contains instructions about how
to convert the :file:`example_scores.xml` file to a CSV format,
and output it to a new file :file:`example_scores.csv`.
The file is shown in its entirety below so that it can be described with
appropriate context::

    <ScoresAwk>
      <FileInput fileName="../results/example_scores.xml" excludeTag="output" />
    
      <PythonFunction condition="BEGIN" action="makeHeader">
        <![CDATA[
    
    # The code goes here, between the braces.
    # It should be exactly the same as in a Python script.
    # Whatever is returned will be written to the output file.
    
    def makeHeader():
        return "event, date, price, price_score, price_alert, gallons, gallons_score\n"
    
        ]]>
      </PythonFunction>
    
      <PythonFunction condition="notEmpty" action="getRow">
        <![CDATA[
    
    def notEmpty(event):
        return len(event.children) > 0
    
    def getContent(seg, tagName):
        return seg.child(tagName).content()
    
    def getRow(event):
        event_no = event["number"]
        the_date = " "
        price = " "
        price_score = " "
        price_alert = " "
        gallons = " "
        gallons_score = " "
    
        for segment in event:
            if segment["id"] == "pricePerGal-zValue":
                the_date = getContent(segment, "date")
                price = getContent(segment, "pricePerGal")
                price_score = getContent(segment, "score")
                price_alert = getContent(segment, "alert")
    
            elif segment["id"] == "gallons":
                gallons = getContent(segment, "gallons")
                gallons_score = getContent(segment, "score")
    
        return ", ".join([
            event_no,
            the_date,
            price,
            price_score,
            price_alert,
            gallons,
            gallons_score]) + "\n"
    
        ]]>
      </PythonFunction>
    
      <FileOutput fileName="../results/example_scores.csv" />
    </ScoresAwk>

It is in XML format, so tags surround information that is
communicated to the program.  The outer tag ``<ScoresAwk>``
just names the program. The entry::

      <FileInput fileName="../results/example_scores.xml" excludeTag="output" />

identifies the location of the input file.  The attribute ``excludeTag``
tells the program to ignore the opening and closing tag in the
:file:`example_scores.xml` file. We named it 'output' in our configuration
file::

    <Output>
        <ToFile name="../results/example_scores.xml" overwrite="true" />
        <ReportTag name="output" />
    </Output>

If instead the ``ReportTag`` tag was deleted, there would be no 'output' tag
wrapping the output in :file:`example_scores.xml` and the ``FileInput`` entry
in the ScoresAwk configuration file would look like::

      <FileInput fileName="../results/example_scores.xml" />

without the ``excludeTag`` attribute. At the bottom of the file is
another entry::

      <FileOutput fileName="../results/example_scores.csv" />

that states where to put the file output.  In between are the instructions
that tell ScoresAwk how to convert the XML file to CSV.  The text inside
of the tags is actually Python code that will be given directly to
Python to be run::

      <PythonFunction condition="BEGIN" action="makeHeader">
        <![CDATA[

        # The code goes here, between the braces.
        # It should be exactly the same as in a Python script.
        # Whatever is returned will be written to the output file.

        def makeHeader():
            return "event, date, price, price_score, price_alert, gallons, gallons_score\n"

        ]]>
      </PythonFunction>

The statement above directs ScoresAwk to run the Python function named
``makeHeader`` (the action) when the condition ``BEGIN`` is met (at the
beginning, before executing any other code).
The ``condition`` attribute can be ``BEGIN``, or ``END``, or the name
of a function that the user defines in between the ``<![CDATA[ ... ]]>`` brackets.
If there is no ``PythonFunction`` element that has condition ``BEGIN``,
then nothing would be done in the beginning (so in this case,
there would be no header row in the file).  Likewise, if there is no need
to do anything after processing the file, there is no need to have any
``PythonFunction`` element with an ``END`` condition.

In Python, spaces are meaningful: everything has to be indented the same
number of spaces, so if you copy the code keep the indentation the same.
Python has a great online manual at `<http://docs.python.org/tutorial/>`_
for those who want to do more things than copying and pasting.
The way to define a function in Python is to write::

   def functionName():
      # function content goes here (indented by 4 spaces;
      # be careful of tabs --- they will mess you up!)
      print "This is a print statement"
      return "String formatting is like in C, so \n is newline and \t is tab."

The long part of the script is for going through the XML output
file row by row. The ``condition`` attribute here tests whether the
Event tag for the row contains any information. If it does, then the
``action`` will be applied::

      <PythonFunction condition="notEmpty" action="getRow">
        <![CDATA[
    
    def notEmpty(event):
        return len(event.children) > 0
    
    def getContent(segment, tagName):
        return segment.child(tagName).content()
    
    def getRow(event):
        event_no = event["number"]
        the_date = " "
        price = " "
        price_score = " "
        price_alert = " "
        gallons = " "
        gallons_score = " "
    
        for segment in event:
            if segment["id"] == "pricePerGal-zValue":
                the_date = getContent(segment, "date")
                price = getContent(segment, "pricePerGal")
                price_score = getContent(segment, "score")
                price_alert = getContent(segment, "alert")
    
            elif segment["id"] == "gallons":
                gallons = getContent(segment, "gallons")
                gallons_score = getContent(segment, "score")
    
        return ", ".join([
            event_no,
            the_date,
            price,
            price_score,
            price_alert,
            gallons,
            gallons_score]) + "\n"
    
        ]]>
      </PythonFunction>


The function ``getContent`` above is neither the ``condition`` nor the ``action``;
but it is called inside of the ``getRow`` function...the point is to demonstrate
that the content between the ``<![CDATA[ ... ]]>`` tags can really contain any
Python code. To run the postprocessing script, type the following at the command
prompt::

   ScoresAwk scores_to_csv.xcfg

There should not be any output.  When the program is done, the directory
:file:`{<Install-dir>}/augustus-examples/gaslog/results/` should contain
the new :file:`example_scores.csv` file.

Congratulations; you have successfully run an Augustus as a PMML consumer!


To more deeply understand what is happening, there needs to be a little explanation
about how the XML is being processed. One of the core utilities in the Augustus
release is a tool for parsing XML. For the curious, it is located in
:file:`{<Install-dir>}/augustus-scoringengine/augustus/core/xmlbase.py`.
ScoresAwk uses this tool. Inside the post processing directory is
:file:`explore_xmlbase.py` which demonstrate a few properties of XML elements
that are helpful when making one's own ScoresAwk configuration file. It is under
:file:`{<Install-dir>}/augustus-examples/gaslog/introductory/postprocess`.  To run it,
you need to have already created the file

:file:`{<Install-dir>}/augustus-examples/gaslog/introductory/results/example_scores.xml`
by running Augustus as a consumer.  At the command prompt type::

   $ python explore_xmlbase.py

to be guided through some of the basics of Augustus' xmlbase library.

.. rubric:: Footnotes

.. [#f1]
         Open Data Group is a member of the Data Mining Group; the group that
         manages the PMML standard.
