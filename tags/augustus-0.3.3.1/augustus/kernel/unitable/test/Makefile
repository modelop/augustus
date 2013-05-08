
OUTDIR = _out
REFDIR = ref
DATADIR = data

VPATH = $(DATADIR)
PYTHON = python
UNITABLE = ../command.py

HAS_DOCTEST =
HAS_DOCTEST += asarray.py
HAS_DOCTEST += wrappers.py
HAS_DOCTEST += storage.py
HAS_DOCTEST += unitable.py
HAS_DOCTEST += evaltbl.py
HAS_DOCTEST += rules.py
DOCTESTS = $(addprefix ../,$(HAS_DOCTEST))

OUT =
OUT += $(OUTDIR)/count.tbl
OUT += $(OUTDIR)/count-expanded.csv
OUT += $(OUTDIR)/simple.csv
OUT += $(OUTDIR)/simple.nab
OUT += $(OUTDIR)/simple-squeezed.nab
OUT += $(OUTDIR)/simple.tbl
OUT += $(OUTDIR)/exploitApply.csv
OUT += $(OUTDIR)/exploitApply.nab
OUT += $(OUTDIR)/exploitApply.tbl
OUT += $(OUTDIR)/rule-demo.out
OUT += $(OUTDIR)/top-ten.out
OUT += $(OUTDIR)/cmd-query.out

all: check
	@echo "---------------------------------------------------------------------"
	@echo "Successful... cleaning up."
	@echo "---------------------------------------------------------------------"
	$(MAKE) clean

check: clean $(OUT)
	@diff -rqs --exclude=.svn --exclude=*.err $(REFDIR) $(OUTDIR)

recheck: $(OUT)
	@diff -rqs --exclude=.svn --exclude=*.err $(REFDIR) $(OUTDIR)

########################################################################
# Note that the doctests are not run automatically because they
# report errors on 64-bit machines and when optional packages are not
# present.  These are errors in the tests and not the code being tested.

doctest: test-docs.py
	PYTHONPATH=../../../.. $(PYTHON) $<

doctest-old: $(DOCTESTS)
	@for x in $^; do \
	  if $(PYTHON) $$x; then \
	    echo "$$x	doctest OK"; \
	  else \
	    echo "$$x	doctest FAILED" && exit 1; \
	  fi; \
	done

########################################################################

%/top-ten.out: sample.nab
	@mkdir -vp $(@D)
	./top-ten.py $^ >$@

%/rule-demo.out: sample.nab
	@mkdir -vp $(@D)
	./rule-demo.py $^ >$@

%/count.tbl: count.csv $(UNITABLE)
	@mkdir -vp $(@D)
	$(UNITABLE) -v --csv --sep=: -o $@ $< 2>$@.err

%/count-expanded.csv: count.csv $(UNITABLE)
	@mkdir -vp $(@D)
	$(UNITABLE) -X -v --tbl -o $@ $< 2>$@.err

%/simple.csv: simple.csv $(UNITABLE)
	@mkdir -vp $(@D)
	$(UNITABLE) -v -o $@ $< 2>$@.err

%/simple.nab: simple.csv $(UNITABLE)
	@mkdir -vp $(@D)
	$(UNITABLE) -v --bin -o $@ $< 2>$@.err

%/simple-squeezed.nab: simple.csv $(UNITABLE)
	@mkdir -vp $(@D)
	$(UNITABLE) --save-space -v --bin -o $@ $< 2>$@.err

%/simple.tbl: simple.csv $(UNITABLE)
	@mkdir -vp $(@D)
	$(UNITABLE) --broken-csv -v --tbl -o $@ $< 2>$@.err

%/exploitApply.csv: exploitApply.csv $(UNITABLE)
	@mkdir -vp $(@D)
	$(UNITABLE) -v -o $@ $< 2>$@.err

%/exploitApply.nab: exploitApply.csv $(UNITABLE)
	@mkdir -vp $(@D)
	$(UNITABLE) -v --bin -o $@ $< 2>$@.err

%/exploitApply.tbl: exploitApply.csv $(UNITABLE)
	@mkdir -vp $(@D)
	$(UNITABLE) -v --tbl -o $@ $< 2>$@.err

########################################################################
QUERY_OPTS = --eval="err=actual-predicted; abserr=_.abs(err); err2=err*err"
QEXPR = (abserr.min(),abserr.max(),err2.mean())
QFMT  = 'min_err=%1.5f max_err=%1.1f mean_squared=%1.6f'
QUERY_OPTS += --query="$(QFMT) % $(QEXPR)"

%/cmd-query.out: sample.score $(UNITABLE)
	@mkdir -vp $(@D)
	$(UNITABLE) -v $(QUERY_OPTS) $< >$@ 2>$@.err

########################################################################

clean:
	@rm -rf $(OUTDIR)
