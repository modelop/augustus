docs := augustus-naivebayes-model \
        augustus-baseline-model \
        augustus-regression-model \
	augustus-tree-model \
	augustus-pmml-consumer \
        augustus-project-example \
        augustus-unitable \
        appendices/pmml \
        appendices/producerConfig \
        appendices/segmentation

templates := $(addsuffix .template,$(docs))
htmldocs := $(addsuffix .html,$(docs))

all: $(htmldocs)

%.html:%.template
	python makedoc.py $< $@

show:
	@ls $(templates)
	@ls $(htmldocs)

clean:
	rm -rf $(htmldocs)
