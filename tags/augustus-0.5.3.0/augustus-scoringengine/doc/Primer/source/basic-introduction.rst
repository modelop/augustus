Introduction
============

This section briefly introduces Augustus.  The reader should afterward be able
to identify when Augustus would be useful and should be able to create a simple
project from scratch.

Description
-----------

Augustus is an open source software toolkit for building and scoring statistical
models.  It is written in Python and its most distinctive features are:

    * Ability to be used on sets of :term:`big data`; these
      are data sets that exceed either memory capacity or disk capacity,
      so that existing solutions like :program:`R` or :program:`SAS`
      cannot be used.  Augustus is also perfectly capable of handling
      problems that can fit on one computer.

    * :term:`PMML` compliance and the ability to both:
      
       - produce models with PMML-compliant formats
         (saved with extension :file:`.pmml`).
       - consume models from files with the PMML format.

Augustus has been tested and deployed on serveral operating systems.  It is
intended for developers who work in the financial or insurance industry,
information technology,  or in the science and research communities.

Usage
-----

Augustus produces and consumes Baseline, Cluster, Tree, and Ruleset models.
Currently, it uses an event-based approach to building Tree, Cluster and Ruleset
models that is non-standard.  Standard model producers are schedule for relase
in January of 2011.

Augustus produces and consumes segmented models and can continue training models
on the same input data it is scoring, so that recent events can influence how
future events are scored.  This is a new feature introduced with this (Augustus
0.5) release.  A typical model development and use cycle with
Augustus is as follows:

    1. Identify suitable data with which to construct a new model.
    2. Provide a model schema which proscribes the requirements for the model.
    3. Run the Augustus producer to obtain a new model using training data.
    4. Run the Augustus consumer on new data to effect scoring. 

This tutorial walks through two examples that each highlight different Augustus
features.  Each of the examples is included in the Augustus release, under the
:file:`augustus-examples` directory.


.. TANYA -- This page DONE.
