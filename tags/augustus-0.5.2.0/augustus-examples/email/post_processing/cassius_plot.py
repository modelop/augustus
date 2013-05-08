import numpy

from augustus.core.scoresfile import ScoresFile
from cassius import *

dates = []
values = []
redchi2s = []
probs = []
for event in ScoresFile("_out/scoresout.xml", excludeTag="Report", contentCast={"date": int, "headerLength": float, "chi2": float, "ndf": float, "prob": float}):
    firstSegment = event.child("Segment", exception=False)
    if firstSegment is not None and firstSegment.goodCast:
        if 2220. < firstSegment.child("headerLength").value < 3000.:
            dates.append(firstSegment.child("date").value)
            values.append(firstSegment.child("headerLength").value)
            redchi2s.append(firstSegment.child("chi2").value / firstSegment.child("ndf").value)
            probs.append(1. - firstSegment.child("prob").value)

binboundaries = Grid(horiz=[2220., 2330., 2430., 2510., 2630., 2820., 3000.])

plotdata = TimeSeries(x=dates, y=values, informat=None, outformat="%Y", limit=10000, connector=None, marker="circle", markersize=0.25)
plotscore = TimeSeries(x=dates, y=redchi2s, informat=None, outformat="%Y", limit=10000, linecolor="red", ylabel="chi2/ndf")

draw(Layout(2, 1, Overlay(1, binboundaries, plotdata), plotscore), fileName="_out/plot.svg")
