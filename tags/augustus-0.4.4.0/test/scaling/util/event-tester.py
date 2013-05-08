from augustus.pmmllib.pmmlConsumer import *
import sys


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print "One and only one argument is required."
        sys.exit(1)
  
    #Parse the config file and init the model.
    con = main('../configs/ev-config.xml')
  
    in_file = '../_data/' + sys.argv[1] + '.csv'
    a = UniTable().from_csv_file(in_file, header="id,date,site,flag,entity")

    hold_results = []

    for x in a:
        #score returns a string like:
        # "<Highway><Event><Direction>S</Direction><Volume>1</Volume><Score>0.888888888889</Score><Alert>True</Alert></Event></Highway>\n"
        hold_results.append( con.score(x) )

