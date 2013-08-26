REGISTER pig/lib/caissepop-1.2*.jar;

DEFINE ComputeTfIdf com.fujitsu.ca.fic.caissepop.evaluation.ComputeTfIdf();


data = LOAD '$INPUT' USING PigStorage(',') as (tfcount:long,ndocs:long,dfcount:long);
out = FOREACH data GENERATE ComputeTfIdf($0,$1,$2);
dump out;

--rmf $OUTPUT
--STORE out into '$OUTPUT' USING PigStorage(',');