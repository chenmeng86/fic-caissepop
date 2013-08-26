package com.fujitsu.ca.fic.caissepop.evaluation;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fujitsu.ca.fic.caissepop.util.Util;

import datafu.pig.util.SimpleEvalFunc;

/**
 * Pig UDF that will compute the BNS (Bi-Normal Seperation) value of a token
 * from the following values: tp, pos, fp and neg.
 * <ul>
 * <li>TP: count of documents within the positives examples where the term
 * appears</li>
 * <li>POS: the count of all positive documents</li>
 * <li>TN: count of documents within the negatives examples where the term
 * appears</li>
 * <li>NEG: the count of all negative documents</li>
 * </ul>
 * 
 * @author dumoulma
 * 
 */
public class ComputeBns extends SimpleEvalFunc<Double> {
	private static final int PRECISION = 5;
	private final Logger log = LoggerFactory.getLogger(ComputeTfIdf.class);

	public Double call(Long tp, Long pos, Long fp, Long neg) {
		double bns = Math
				.abs(computeFminus1(tp, pos) - computeFminus1(fp, neg));
		log.debug("BNS score is: " + bns);

		return Util.round(bns, PRECISION);
	}

	private static final double MIN_TPR_LIMIT = 0.0005;
	private static final NormalDistribution NORMAL_DISTRIBUTION = new NormalDistribution();

	private double computeFminus1(long nCasesWithWord, long nCases) {
		double rate = (double) nCasesWithWord / nCases;
		if (rate < MIN_TPR_LIMIT) {
			rate = MIN_TPR_LIMIT;
		} else if (rate > 1 - MIN_TPR_LIMIT) {
			rate = 1 - MIN_TPR_LIMIT;
		}

		return NORMAL_DISTRIBUTION.inverseCumulativeProbability(rate);
	}
}
