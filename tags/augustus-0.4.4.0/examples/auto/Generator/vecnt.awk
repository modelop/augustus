BEGIN{
}
{
   date=$1;
   color=$2;
   for (i=0;i<5;i++) {
     make=$(3+i+i);
     cnt=$(4+i+i);
     tot[color,make]+=cnt;
     mtot[make]+=cnt;
     ctot[color]+=cnt;
   }
}
END{
   print "Totals by color"
   for (c in ctot) { print c, ctot[c]; }
   print "Totals by make"
   for (m in mtot) { print m, mtot[m]; }
   print "Totals by make,color"
   for (m in mtot) {
     for (c in ctot) {
       print m,c,tot[c,m];
     }
   }
}
