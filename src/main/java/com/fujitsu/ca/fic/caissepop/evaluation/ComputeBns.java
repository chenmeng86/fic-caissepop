package com.fujitsu.ca.fic.caissepop.evaluation;

import java.io.IOException;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fujitsu.ca.fic.caissepop.util.Util;

/**
 * Pig UDF that will compute the BNS (Bi-Normal Seperation) value of a token from the following values: tp, pos, fp and neg.
 * <ul>
 * <li>TP: count of documents within the positives examples where the term appears</li>
 * <li>POS: the count of all positive documents</li>
 * <li>TN: count of documents within the negatives examples where the term appears</li>
 * <li>NEG: the count of all negative documents</li>
 * </ul>
 * 
 * @author dumoulma
 * 
 */
public class ComputeBns extends EvalFunc<Double> {
    private final Logger log = LoggerFactory.getLogger(ComputeTfIdf.class);

    @Override
    public Double exec(Tuple input) throws IOException {
        if (input == null || input.size() != 4) {
            log.warn("ComputeBns: input size is != 4 or input is null!");
            return null;
        }
        if (input.isNull(0) || input.isNull(1) || input.isNull(2) || input.isNull(3)) {
            log.warn("ComputeBns: either of the input values isNull()");
            return null;
        }

        long tp = (Long) input.get(0);
        long pos = (Long) input.get(1);
        long fp = (Long) input.get(2);
        long neg = (Long) input.get(3);

        double bns = Math.abs(computeFminus1(tp, pos) - computeFminus1(fp, neg));
        log.debug("BNS score is: " + bns);

        return Util.round(bns, 5);
    }

    private static final double MIN_TPR_LIMIT = 0.0005;
    private static final NormalDistribution NORMAL_DISTRIBUTION = new NormalDistribution();

    private double computeFminus1(long nCasesWithWord, long nCases) throws IOException {
        double rate = (double) nCasesWithWord / nCases;
        if (rate < MIN_TPR_LIMIT) {
            rate = MIN_TPR_LIMIT;
        } else if (rate > 1 - MIN_TPR_LIMIT) {
            rate = 1 - MIN_TPR_LIMIT;
        }

        return NORMAL_DISTRIBUTION.inverseCumulativeProbability(rate);
    }
}
