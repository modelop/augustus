TMPDIR=_out

all: produceAndConsume

$(TMPDIR):
	mkdir $(TMPDIR)

produceAndConsume: $(TMPDIR)
	Augustus config/email_headers_config.xcfg
	ScoresDiff $(TMPDIR)/scoresout.xml ref/scoresout.xml
# 	ScoresAwk post_processing/scores_to_unitable.xcfg
	ScoresAwk post_processing/scores_to_csv.xcfg

clean:
	rm -rf $(TMPDIR)
