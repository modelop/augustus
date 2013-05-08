OUTDIR = _out
REFDIR = ref
DATADIR = data

PYTHON = python

all: check clean

clean:
	@rm -rf $(OUTDIR)

check: create
	@diff -rqs --exclude=.svn --exclude=*.err $(REFDIR)/out.pmml $(OUTDIR)/out.pmml
	@diff -rqs --exclude=.svn --exclude=*.err $(REFDIR)/out2.pmml $(OUTDIR)/out2.pmml

create: 1 2

1:
	@mkdir -vp $(OUTDIR)
	$(PYTHON) ../producer/Producer.py -c $(DATADIR)/producer.xml
	$(PYTHON) ../consumer/Consumer.py -c $(DATADIR)/consumer.xml

2:
	@mkdir -vp $(OUTDIR)
	$(PYTHON) ../producer/Producer.py -c $(DATADIR)/producer2.xml
	$(PYTHON) ../consumer/Consumer.py -c $(DATADIR)/consumer2.xml
