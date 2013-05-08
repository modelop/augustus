#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>

char ** getdates(int);
int *getsamp(int,int);
int *getperm(int);
int *identity(int);
static char *segname[]={"Red","Blue","Green","Black"};
#define NSEG (sizeof(segname)/sizeof(char *))
static char *catname[]={"Toyota","Mazda","BMW","Audi","Volvo"};
#define NCAT (sizeof(catname)/sizeof(char *))

struct event {
  int category;
  int segment;
};

int
main(int argc, char **argv)
{
   int i,j,k,c,ntot=1000,ndays=10,ncat=5,weights=0;
   int evx,sx,cx;
   char **dates;
   char *csvpath="dat.csv";
   char *vecpath="dat.vec";
   FILE *csvfile,*vecfile;
   int *daysamp,*segsamp,*catsamp,*evperm;
   int seed=time(0);
   while ((c=getopt(argc,argv,"n:d:c:v:s:w"))!=EOF) {
     switch(c) {
     case 'n':ntot=atoi(optarg);break;
     case 'd':ndays=atoi(optarg);break;
     case 'c':csvpath=optarg;break;
     case 'v':vecpath=optarg;break;
     case 's':seed=atoi(optarg);break;
     case 'w':weights++;break;
     default:
	exit(1);
     }
   }
   srand48(seed);
   if ((csvfile=fopen(csvpath,"w"))==NULL)
      fprintf(stdout,"error opening csv file (%s)\n",csvpath),exit(1);
   if (weights)
      fprintf(csvfile,"Count, ");
   fprintf(csvfile,"Date, Color, Automaker\n");
   if ((vecfile=fopen(vecpath,"w"))==NULL)
      fprintf(stdout,"error opening vec file (%s)\n",vecpath),exit(1);

   dates=getdates(ndays);
   daysamp=getsamp(ndays,ntot);
   for (j=0;j<ndays;j++) {
      fprintf(stdout,"%s, %d\n",dates[j],daysamp[j]);
      if (daysamp[j]) {
	 struct event *
	 ev=(struct event *)calloc(sizeof(struct event),daysamp[j]);
	 evx=0;
         segsamp=getsamp(NSEG,daysamp[j]);
	 for (sx=0;sx<NSEG;sx++) {
	    fprintf(stdout,"\t%6s %d\n",segname[sx],segsamp[sx]);
	    if (segsamp[sx]) {
	       catsamp=getsamp(NCAT,segsamp[sx]);
	       for (cx=0;cx<NCAT;cx++) {
	          fprintf(stdout,"\t\t%6s %d\n",catname[cx],catsamp[cx]);
		  for(c=0;c<catsamp[cx];c++) {
		     ev[evx].category=cx;
		     ev[evx].segment=sx;
		     evx++;
		  }
	       }
	       fprintf(vecfile,"%s:%s:",dates[j],segname[sx]);
	       for (cx=0;cx<NCAT;cx++) {
	          fprintf(vecfile,"%s:%d",catname[cx],catsamp[cx]);
	          if (cx<NCAT-1)fputc(':',vecfile);
	       }
	       fprintf(vecfile,"\n");
	       free(catsamp);
	    }
	 }
	 free(segsamp);
	 evperm=getperm(daysamp[j]);
	 if (weights) {
	    free(evperm);
	    evperm=identity(daysamp[j]);
	 }
	 i=0;
	 while(i<daysamp[j]) {
	    int sn,cn;
	    sn=ev[evperm[i]].segment;
	    cn=ev[evperm[i]].category;
	    if (weights) {
	       k=1;
	       while ((i+k)<daysamp[j] &&
	          sn==ev[evperm[i+k]].segment &&
	          cn==ev[evperm[i+k]].category){
		  k++;
	       }
	       fprintf(csvfile,"%d,%s,%6s,%6s\n", k, dates[j], 
	       segname[sn],catname[cn]);
	       i+=k;
	    } else {
	       fprintf(csvfile,"%s,%6s,%6s\n", dates[j], 
	       segname[sn],catname[cn]);
	       i++;
	    }
	 }
	 free(evperm);
	 free(ev);
      }
   }
   fclose(csvfile);
   fclose(vecfile);
   exit(0);
}

int *
identity(int n)
{
   int *p,i,k,t;
   p = (int *)calloc(sizeof(int),n);
   for (i=0;i<n;i++)
     p[i]=i;
   return p;
}
int *
getperm(int n)
{
   int *p,i,k,t;
   p = (int *)calloc(sizeof(int),n);
   for (i=0;i<n;i++)
     p[i]=i;
   for (i=0;i<n;i++) {
     t=drand48()*(n-i);
     k=i+t;
     t=p[k];p[k]=p[i];p[i]=t;
   }
   return p;
}
int *
getsamp(bins,tot)
{
   int *s,i,t;
   double *f,sum=0;
   s = (int *)calloc(sizeof(int),bins);
   f = (double *)calloc(sizeof(double),bins);
   for (i=0;i<bins;i++){
      f[i]=drand48();
      sum+=f[i];
   }
   for (t=i=0;i<bins;i++)
      t+=s[i]=(tot*f[i]/sum);
   for (i=0;i<tot-t;i++)
     s[(int)(drand48()*bins)]++;
   free(f);
   return s;
}

char **
getdates(int nd)
{
   static char *ms[]={ "Jan", "Feb", "Mar", "Apr", "May", "Jun", 
			"Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
   static int md[]={31,28,31,30,31,30,31,31,30,31,30,31};

   char **d;
   int month,day,year=2000,j;
   d=(char **)calloc(sizeof(char *),nd);
   month=(int)(drand48()*12);
   day=1+(int)(drand48()*md[month]);
   for (j=0;j<nd;j++) {
     d[j]=calloc(sizeof(char),32);
     sprintf(d[j],"%4d-%02d-%02d",year,month+1,day);
     day+=1;
     if (day>(((year%4)||(month!=1))?md[month]:29)) {
       day=1;
       if (++month==12){
         year++;
	 month=0;
       }
     }
   }
   return d;
}

