REGISTER target/caissepop-1.2.jar;
REGISTER pig/lib/commons-math3-3.2.jar;
REGISTER pig/lib/lucene-*.jar.jar;

DEFINE TokenizeText com.fujitsu.ca.fic.caissepop.evaluation.TokenizeText();
DEFINE BNS com.fujitsu.ca.fic.caissepop.evaluation.ComputeBns();

positiveDocs    = LOAD 'data/5docs' USING PigStorage('\n','-tagsource') 
                     AS (doc_id:chararray, text:chararray);

posTokens = FOREACH positiveDocs GENERATE doc_id, 
             FLATTEN(TokenizeText(text)) 
             AS (token:chararray);
posTokens = FILTER posTokens BY token MATCHES '\\w.*';

negativeDocs = LOAD 'data/neg' USING PigStorage('\n','-tagsource') 
                     AS (doc_id:chararray, text:chararray);
negTokens = FOREACH negativeDocs GENERATE doc_id, 
             FLATTEN(TokenizeText(text)) 
             AS (token:chararray);
negTokens = FILTER negTokens BY token MATCHES '\\w.*';

vocabUnion = UNION posTokens, negTokens;

-- count the number of positive and negative documents
posDocs = FOREACH posTokens GENERATE doc_id;
posDocs = DISTINCT posDocs;
posDocs = FOREACH (GROUP posDocs ALL) 
    GENERATE COUNT(posDocs) AS n_docs;

negDocs = FOREACH negTokens GENERATE doc_id;
negDocs = DISTINCT negDocs;
negDocs = FOREACH (GROUP negDocs ALL) 
    GENERATE COUNT(negDocs) AS n_docs;
    
 -- count the true positive and false positive counts for each vocabulary token
posNegGrouped = COGROUP posTokens BY token, negTokens BY token;

 
countsPipe = FOREACH posNegGrouped {
    posTokens = posTokens.token;
    negTokens = negTokens.token;
    GENERATE group AS token, COUNT(posTokens) AS tp, posDocs.n_docs AS pos,
    COUNT(negTokens) AS fp, negDocs.n_docs AS neg;
}
countsPipe = FILTER countsPipe BY tp + fp < 3;

bnsScored = FOREACH countsPipe GENERATE token, BNS(tp,pos,fp,neg) AS bns, tp + fp as overall_count;   

tokenScoredJoined = JOIN vocabUnion BY token, bnsScored BY token;
tokenScoredGrouped = GROUP tokenScoredJoined BY doc_id;

rmf $OUTPUT/out/bns-map
rmf $OUTPUT/out/bns-docs-scored
STORE bnsScored INTO '$OUTPUT/out/bns-map' USING PigStorage(',','schema');
STORE tokenScoredGrouped INTO '$OUTPUT/out/bns-docs-scored' USING PigStorage(',','schema');