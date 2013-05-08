from cassius import *

email = inspect("jims_email.nab")

h = email.histogram("headerLength", "mailinglist == 'T'", numbins=100, lowhigh=(2200, 3000), fillcolor="yellow")

draw(Overlay(0, h, Grid(vert=[2330., 2430., 2510., 2630., 2820.])), fileName="_out/headerLength.svg")
