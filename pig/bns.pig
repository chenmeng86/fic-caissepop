REGISTER target/caissepop-1.2.jar;
REGISTER pig/lib/commons-math3-3.2.jar;
REGISTER pig/lib/lucene-*.jar;

%default MIN_COUNT 2
%default MAX_VOCAB_SIZE 5000
%default MIN_BNS_SCORE 0.00001

DEFINE TokenizeText com.fujitsu.ca.fic.caissepop.evaluation.TokenizeText();
DEFINE BNS com.fujitsu.ca.fic.caissepop.evaluation.ComputeBns();

positiveDocs    = LOAD '$INPUT_POS' USING PigStorage('\n','-tagsource') 
                     AS (doc_id:chararray, text:chararray);

posTokens = FOREACH positiveDocs GENERATE doc_id, 
             FLATTEN(TokenizeText(text)) 
             AS (token:chararray);
posTokens = FILTER posTokens BY token MATCHES '\\w.*';
posTokens = FILTER posTokens BY SIZE(token) > 1;

negativeDocs = LOAD '$INPUT_NEG' USING PigStorage('\n','-tagsource') 
                     AS (doc_id:chararray, text:chararray);
negTokens = FOREACH negativeDocs GENERATE doc_id, 
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
        BNS(tp, posDocs.n_docs, fp, negDocs.n_docs) as bns, 
        all_count as all_count:long;
}
bnsPipe = FILTER bnsPipe BY all_count > $MIN_COUNT OR bns > $MIN_BNS_SCORE;
bnsPipe = ORDER bnsPipe BY bns DESC;
bnsPipe = LIMIT bnsPipe $MAX_VOCAB_SIZE;

tokenScoredJoined = JOIN vocabUnion BY token, bnsPipe BY token;
tokenScoredGrouped = GROUP tokenScoredJoined BY doc_id;

rmf $OUTPUT/bns-map
rmf $OUTPUT/bns-docs-scored
STORE bnsPipe INTO '$OUTPUT/bns-map' USING PigStorage(',','schema');
STORE tokenScoredGrouped INTO '$OUTPUT/bns-docs-scored' USING PigStorage(',','schema');