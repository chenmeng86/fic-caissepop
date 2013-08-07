REGISTER lib/caissepop-1.2.jar;
REGISTER lib/commons-math3-3.2.jar;
REGISTER lib/lucene-*.jar;

DEFINE TokenizeText com.fujitsu.ca.fic.caissepop.evaluation.TokenizeText();
DEFINE BNS com.fujitsu.ca.fic.caissepop.evaluation.ComputeBns();

positiveDocs    = LOAD 'data/pos' USING PigStorage('\n','-tagsource') 
                     AS (doc_id:chararray, text:chararray);

posTokens = FOREACH positiveDocs GENERATE doc_id, 1 as label:int,
             FLATTEN(TokenizeText(text)) 
             AS (token:chararray);
posTokens = FILTER posTokens BY token MATCHES '\\w.*';
posTokens = FILTER posTokens BY SIZE(token) > 1;

negativeDocs = LOAD 'data/neg' USING PigStorage('\n','-tagsource') 
                     AS (doc_id:chararray, text:chararray);
negTokens = FOREACH negativeDocs GENERATE doc_id, 0 as label:int,
             FLATTEN(TokenizeText(text)) 
             AS (token:chararray);
negTokens = FILTER negTokens BY token MATCHES '\\w.*';
negTokens = FILTER negTokens BY SIZE(token) > 1;

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
 
bnsPipe = FOREACH posNegGrouped {
    posTokens = posTokens.token;
    negTokens = negTokens.token;
    tp = COUNT(posTokens);
    fp = COUNT(negTokens); 
    all_count = (tp + fp);
    GENERATE group AS token, 
        BNS(tp, posDocs.n_docs, fp, negDocs.n_docs) as bns:double, 
        all_count as all_count:long;
}
bnsPipe = FILTER bnsPipe BY all_count > 0 AND bns > 0.1;
bnsPipe = ORDER bnsPipe BY bns DESC;
bnsPipe = LIMIT bnsPipe 10;

tokenScoredJoined = JOIN vocabUnion BY token, bnsPipe BY token;
tokenScoredJoined = DISTINCT tokenScoredJoined;
tokenScoredGrouped = GROUP tokenScoredJoined BY doc_id;