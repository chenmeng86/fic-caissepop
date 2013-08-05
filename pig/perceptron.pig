%default DATA_DIR ../data/10vectors
%default OUTPUT_DIR ../data/perceptron-pig-output

REGISTER '../target/mltp2udf-1.12.jar';
REGISTER 'lib/mahout-math-0.7.jar';
REGISTER 'lib/mahout-core-0.7.jar';

DEFINE SCORE_WITH_PERCEPTRON ca.ulaval.ift7002.tp2.udf.ScoreWithPerceptron();
DEFINE VECTORIZE_SERVICE ca.ulaval.ift7002.tp2.udf.VectorizeService();
DEFINE VECTORIZE_PROTOCOL_TYPE ca.ulaval.ift7002.tp2.udf.VectorizeProtocolType();
DEFINE VECTORIZE_FLAG ca.ulaval.ift7002.tp2.udf.VectorizeFlag();
DEFINE VECTORIZE_LABEL ca.ulaval.ift7002.tp2.udf.VectorizeBinaryLabel();

set perceptron.model.path '../model/perceptron.model'

rmf $OUTPUT_DIR

kddData = LOAD '$DATA_DIR' USING PigStorage(',') 
    AS (label:int, duration:int, protocol_type:int, service:int, flag:int, 
    src_bytes:int,dst_bytes:int, land:int, wrong_fragment:int, 
    urgent:int, hot:int,num_failed_logins:int, logged_in:int, 
    num_compromised:int, root_shell: int, su_attempted:int, num_root:int, 
    num_file_creations: int, num_shells:int, num_access_files:int, 
    num_outbound_cmds:int, is_host_login:int, is_guest_login:int, 
    count:int, srv_count:int, serror_rate: double, srv_serror_rate: double, 
    rerror_rate: double, srv_rerror_rate: double, same_srv_rate: double, diff_srv_rate: double,
    srv_diff_host_rate: double, dst_host_count: double, dst_host_srv_count: double, 
    dst_host_same_srv_rate: double, dst_host_diff_srv_rate: double,
    dst_host_same_src_port_rate: double, dst_host_srv_diff_host_rate: double, 
    dst_host_serror_rate: double, dst_host_srv_serror_rate: double, 
    dst_host_rerror_rate: double, dst_host_srv_rerror_rate: double);
P = FOREACH kddData GENERATE SCORE_WITH_PERCEPTRON(*) as confidence:double, $0..;
dump P;

--STORE dataWithConfidence INTO '$OUTPUT_DIR' USING PigStorage(',');
