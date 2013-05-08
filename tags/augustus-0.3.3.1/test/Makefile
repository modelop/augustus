TESTDIRS =
TESTDIRS += ../augustus/kernel/unitable/test
TESTDIRS += ../augustus/modellib/baseline/test
TESTDIRS += ../augustus/modellib/tree/test

.PHONY: all check clean
all check clean:
	for dir in $(TESTDIRS); do $(MAKE) -C$$dir; done

