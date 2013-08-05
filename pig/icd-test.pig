REGISTER target/caissepop-1.2.jar;
REGISTER pig/lib/commons-math3-3.2.jar;

DEFINE ICD com.fujitsu.ca.fic.caissepop.evaluation.InverseCumDist();

rmf $OUTPUT

data = LOAD '$INPUT' USING PigStorage(',') as (a:long,b:long);
out = FOREACH data GENERATE ICD($0,$1);

dump out;
--STORE out into '$OUTPUT' USING PigStorage(',');
