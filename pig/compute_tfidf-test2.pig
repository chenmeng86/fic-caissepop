REGISTER pig/lib/caissepop-1.2*.jar;
REGISTER pig/lib/lucene-core-4.4.0.jar
REGISTER pig/lib/lucene-analyzers-3.6.2.jar;

define TokenizeText com.fujitsu.ca.fic.caissepop.evaluation.TokenizeText();
define TFIDF com.fujitsu.ca.fic.caissepop.evaluation.ComputeTfIdf();

documents    = LOAD '$INPUT' USING PigStorage('\n', '-tagsource') 
                AS (doc_id:chararray, text:chararray);

tokenPipe = FOREACH documents GENERATE doc_id, 
             FLATTEN(TokenizeText(text)) 
             AS (token:chararray);
tokenPipe = FILTER tokenPipe BY token MATCHES '\\w.*';

dPipe = FOREACH tokenPipe GENERATE doc_id;
dPipe = DISTINCT dPipe;
dGroups = GROUP dPipe ALL;
dPipe = FOREACH dGroups {
  GENERATE COUNT(dPipe) AS n_docs;
}

tfGroups = GROUP tokenPipe BY (doc_id, token);
tfPipe = FOREACH tfGroups 
            GENERATE FLATTEN(group) 
            AS (doc_id, tf_token), COUNT(tokenPipe) AS tf_count:long;

tokenGroups = GROUP tokenPipe BY token;
dfPipe = FOREACH tokenGroups {
  dfPipe = distinct tokenPipe.doc_id;
  GENERATE group AS df_token, COUNT(dfPipe) AS df_count;
}

-- join to bring together all the components for calculating TF-IDF
tfidfPipe = JOIN tfPipe BY tf_token, dfPipe BY df_token;


tfidfPipe = FOREACH tfidfPipe GENERATE doc_id, tf_token AS token, 
  TFIDF(tf_count, dPipe.n_docs, df_count) as tfidf:double, df_count, tf_count;

dump tfidfPipe;
