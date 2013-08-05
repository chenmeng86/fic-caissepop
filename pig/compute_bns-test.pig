REGISTER target/caissepop-1.2.jar;
REGISTER pig/lib/commons-math3-3.2.jar;

DEFINE BNS com.fujitsu.ca.fic.caissepop.evaluation.ComputeBns();

rmf $OUTPUT

data = LOAD '$INPUT' USING PigStorage(',') as (tp:long,pos:long,fp:long,neg:long);
out = FOREACH data GENERATE BNS(tp,pos,fp,neg);
dump out;
--STORE out into '$OUTPUT' USING PigStorage(',');