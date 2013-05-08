TMPDIR=_out

all: produceAndConsume

$(TMPDIR):
	mkdir $(TMPDIR)

produceAndConsume: $(TMPDIR)
	# Produce a model using the old car
	Augustus config/producer_config.xcfg
	# Make it apply to the new car
	PmmlSed manipulation/postproducer_replacements.xcfg
	# Score the new car: are the miles driven, MPG, etc. consistent with the old?
	Augustus config/consumer_config.xcfg

	# Verify that these values match the reference data
	PmmlDiff --validate $(TMPDIR)/producer_output.pmml ref/producer_output.pmml
	PmmlDiff --validate $(TMPDIR)/postproducer_output.pmml ref/postproducer_output.pmml
	ScoresDiff $(TMPDIR)/consumer_output.xml ref/consumer_output.xml

	# Update the old-car model using new-car data
	Augustus config/secondproducer_config.xcfg
	# Produce a new model from scratch using old-car and new-car data
	# This should yield the same results
	Augustus config/bothproducer_config.xcfg

	# The secondproducer and bothproducer necessarily have different predicates: just cut them out with pmmlSed
	PmmlSed manipulation/stripboth_for_comparison.xcfg < $(TMPDIR)/secondproducer_output.pmml > $(TMPDIR)/secondproducer_stripped.pmml
	PmmlSed manipulation/stripboth_for_comparison.xcfg < $(TMPDIR)/bothproducer_output.pmml > $(TMPDIR)/bothproducer_stripped.pmml
	# Now we can directly verify that secondproducer and bothproducer yield the same results
	# (They differ in the 10th significant figure because of round-off error from being written to and read from files)
	PmmlDiff --validate --sigfigs 8 $(TMPDIR)/secondproducer_stripped.pmml $(TMPDIR)/bothproducer_stripped.pmml

	# Why not check them against the reference data, too?
	PmmlDiff --validate $(TMPDIR)/secondproducer_output.pmml ref/secondproducer_output.pmml
	PmmlDiff --validate $(TMPDIR)/bothproducer_output.pmml ref/bothproducer_output.pmml

clean:
	rm -rf $(TMPDIR)
