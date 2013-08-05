package com.fujitsu.ca.fic.caissepop.evaluation;

import java.io.IOException;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

/**
 * UDF that will lookup the value of the ratio of true positives to number of positives from the InverseCumulative distribution.
 * 
 * @author dumoulma
 * 
 */
public class InverseCumDist extends EvalFunc<Double> {
    private static final double MIN_TPR_LIMIT = 0.0005;
    static final private NormalDistribution nd = new NormalDistribution();

    @Override
    public Double exec(Tuple input) throws IOException {
        if (input == null || input.size() != 2 || input.isNull(0) || input.isNull(1))
            return null;

        long tp = (Long) input.get(0);
        long pos = (Long) input.get(1);
        if (tp > pos)
            throw new ExecException("tp > pos, so tpr is > 1.0 which is impossible.", PigException.INPUT);

        // Make sure to limit the range of tpr because the inverse normal goes
        // to infinity near 0 and 1
        double tpr = (double) tp / pos;
        if (tpr < MIN_TPR_LIMIT)
            tpr = MIN_TPR_LIMIT;
        else if (tpr > 1 - MIN_TPR_LIMIT)
            tpr = 1 - MIN_TPR_LIMIT;

        return nd.inverseCumulativeProbability(tpr);
    }
}
