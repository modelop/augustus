import glob

from augustus.core.xmlbase import loadfile
import augustus.core.pmml41 as pmml
from cassius import *

modelFiles = glob.glob("_out/modelout*.pmml")
modelFiles.sort()

models = []
for modelFile in modelFiles:
    model = loadfile(modelFile, pmml.X_ODG_PMML)
    models.append(model)

print model.tree()

plots = []
for model in models:
    eventNumber = model.descendant(pmml.X_ODG_Eventstamp)["number"]

    discretize = model.descendant(pmml.Discretize)
    bins = []
    for discretizeBin in discretize.matches(pmml.DiscretizeBin):
        binName = discretizeBin["binValue"]
        leftMargin = discretizeBin.child(pmml.Interval)["leftMargin"]
        rightMargin = discretizeBin.child(pmml.Interval)["rightMargin"]
        bins.append((binName, leftMargin, rightMargin))
    bins.sort(lambda a, b: cmp(a[1], b[1]))

    h = HistogramNonUniform([(b[1], b[2]) for b in bins],
                            fillcolor="yellow",
                            xlabel="headerLength",
                            toplabel="Event %d" % eventNumber,
                            bottommargin=0.1,
                            xlabeloffset=0.1,
                            topmargin=0.07,
                            toplabeloffset=-0.02,
                            )

    countTable = model.descendant(pmml.CountTable)
    for i, (binName, leftMargin, rightMargin) in enumerate(bins):
        fieldValueCount = countTable.child(lambda x: x["value"] == binName, exception=False)
        if fieldValueCount is not None:
            h.values[i] = fieldValueCount["count"]

    plots.append(h)

draw(Layout(6, 5, *plots), width=4000, height=4000, fileName="_out/model_plot.svg")


