.. raw:: latex

    \newpage

.. _gaslog_aim_tutorial:

Quick introduction to AIM
=========================

.. _gaslog_AIM:

It is possible to run Augustus in a mode that simultaneously produces and trains
new segments and scores existing data.  We call this Automatically Incrementing
Models (or AIM) mode.  This is useful when data are constantly streaming, and
where new segments may occur at any time; for example when trying to perform
statistical analysis of activity over various IP addresses. With AIM,

  * Segments are now automatically created as new data are encountered.
  * Segmentation, transformations, and AIM event weighting are independent from the algorithm.
  * Data input and model output are in separate threads from the main processor.

and the user can,

  * Specify weights to the input data, so that recent data have more impact on the model than old data (e.g. exponential blending and moving windows).
  * Weight (or window) consumer data differently from producer data, so that consumers can produce non-trivial results.


Augustus will run in AIM mode if its configuration file contains
both output and scoring specifications. The following elements need
to be added to the current :file:`test_config.xcfg` file to run
in AIM mode::

   <EventSettings output="true" score="true" />

   <Output>
      <ToFile name="../results/scores.xml" overwrite="true" />
      <ReportTag name="Report>
   </Output>

With these added, the following command will create both a scores
file :file:`../results/scores.xml` and the model file
:file:`../models/produced_model.pmml`.::

   $ Augustus test_config.xcfg

The output should look like this (lines are wrapped for display purposes)::

   META INFO     New segment created: (car EQ 'old') and ((intmonth GE 6)
                     and (intmonth LE 8)), ID=Untitled-1
   META INFO     New segment created: (car EQ 'old') and ((intmonth GT 8)
                     and (intmonth LE 10)), ID=Untitled-2
   META INFO     New segment created: (car EQ 'old') and ((intmonth GT 10)
                     and (intmonth LE 12)), ID=Untitled-3
   META INFO     New segment created: (car EQ 'old') and ((intmonth GT 0)
                     and (intmonth LE 5)), ID=Untitled-4
   META INFO     New segment created: (car EQ 'new') and ((intmonth GE 6)
                     and (intmonth LE 8)), ID=Untitled-5
   META INFO     New segment created: (car EQ 'new') and ((intmonth GT 8)
                     and (intmonth LE 10)), ID=Untitled-6
   META INFO     New segment created: (car EQ 'new') and ((intmonth GT 10)
                     and (intmonth LE 12)), ID=Untitled-7
   META INFO     New segment created: (car EQ 'new') and ((intmonth GT 0)
                     and (intmonth LE 5)), ID=Untitled-8
   META INFO     ### Current MetaData content ###
   META INFO     Run time : 0.257986783981


.. rubric:: Footnotes

.. [#f1]
         Open Data Group is a member of the Data Mining Group; the group that
         manages the PMML standard.
