package com.fujitsu.ca.fic.caissepop.evaluation;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;

import com.fujitsu.ca.fic.caissepop.util.Util;

import datafu.pig.util.SimpleEvalFunc;

/**
 * UDF that will lookup the value of the ratio of true positives to number of
 * positives from the InverseCumulative distribution.
 * 
 * @author dumoulma
 * 
 */
public class InverseCumDist extends SimpleEvalFunc<Double> {
	private static final int PRECISION = 5;
	private static final NormalDistribution NORMAL_DISTRIBUTION = new NormalDistribution();
	private static final double MIN_TPR_LIMIT = 0.0005;

	public Double call(Long tp, Long pos) throws ExecException {
		if (tp > pos) {
			throw new ExecException(
					"tp > pos, so tpr is > 1.0 which is impossible.",
					PigException.INPUT);
		}

		// Make sure to limit the range of tpr because the inverse normal goes
		// to infinity near 0 and 1
		double tpr = (double) tp / pos;
		if (tpr < MIN_TPR_LIMIT) {
			tpr = MIN_TPR_LIMIT;
		} else if (tpr > 1 - MIN_TPR_LIMIT) {
			tpr = 1 - MIN_TPR_LIMIT;
		}

		return Util.round(
				NORMAL_DISTRIBUTION.inverseCumulativeProbability(tpr), PRECISION);
	}
}
