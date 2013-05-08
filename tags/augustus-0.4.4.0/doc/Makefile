docs := augustus-naivebayes-model \
        augustus-baseline-model \
        augustus-ruleset-model \
        augustus-tree-model \
        augustus-pmml-consumer \
        augustus-project-example \
        augustus-statistical-considerations \
        augustus-transforms \
        augustus-unitable \
        appendices/pmml \
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
