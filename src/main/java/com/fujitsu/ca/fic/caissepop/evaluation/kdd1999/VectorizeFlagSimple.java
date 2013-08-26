package com.fujitsu.ca.fic.caissepop.evaluation.kdd1999;

import java.util.HashMap;
import java.util.Map;

import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datafu.pig.util.SimpleEvalFunc;

public class VectorizeFlagSimple extends SimpleEvalFunc<Integer> {
	private final Logger log = LoggerFactory.getLogger(VectorizeFlag.class);

	private static final String[] FLAG_NAMES = { "SF", "S0", "S1", "S2", "S3",
			"OTH", "REJ", "RSTO", "RSTR", "RSTOS0", "SH", "RSTRH", "SHR",
			"RESTR_FLAG" };

	private static final Map<String, Integer> flags = new HashMap<String, Integer>();
	{
		for (int i = 0; i < FLAG_NAMES.length; i++) {
			flags.put(FLAG_NAMES[i], i);
		}
	}

	public Integer call(String flagField) throws ExecException {
		if (flagField == null)
			return null;
		if (!flags.containsKey(flagField)) {
			log.warn(flagField + " is an unknown value for the flag field");
			throw new ExecException("Flag Type unknown value: " + flagField,
					PigException.INPUT);
		}
		return flags.get(flagField);
	}
}