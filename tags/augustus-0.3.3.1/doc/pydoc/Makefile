AUGUSTUS = $(CURDIR)/../..
PYTHONPATH  = $(AUGUSTUS)

.PHONY: html clean

html:
	PYTHONPATH=$(PYTHONPATH) pydoc -w $(AUGUSTUS)
	@echo "=========================================================="
	@echo "NOTE: error messages above may be ignored" 
	@echo "=========================================================="
	@cp -fv augustus.html index.html

clean:
	rm -f *.html
