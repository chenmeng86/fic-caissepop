package com.fujitsu.ca.fic.caissepop.evaluation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fujitsu.ca.fic.caissepop.util.Util;

import datafu.pig.util.SimpleEvalFunc;

/**
 * Pig UDF that will compute the TF-IDF value from the term frequency, document
 * frequency and document count.
 * <p>
 * Formula: tf * log (nDocs/df)
 * </p>
 * 
 * @author dumoulma
 * 
 */
public class ComputeTfIdf extends SimpleEvalFunc<Double> {
	private final Logger log = LoggerFactory.getLogger(ComputeTfIdf.class);

	public Double call(Long tfCount, Long nDocs, Long dfCount) {
		double tfidfScore = tfCount * Math.log(nDocs / (1.0 + dfCount));
		log.debug(String.format("tfCount: %d nDocs: %d dfCount: %d", tfCount,
				nDocs, dfCount));

		return Util.round(tfidfScore, 5);
	}
}
