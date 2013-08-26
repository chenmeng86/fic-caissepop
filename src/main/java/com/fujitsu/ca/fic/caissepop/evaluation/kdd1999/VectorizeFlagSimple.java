package com.fujitsu.ca.fic.caissepop.evaluation.kdd1999;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datafu.pig.util.SimpleEvalFunc;

public class VectorizeFlagSimple extends SimpleEvalFunc<Integer> {
	private final Logger log = LoggerFactory.getLogger(VectorizeFlag.class);

	private static final String[] FLAG_NAMES = { "SF", "S0", "S1", "S2", "S3",
			"OTH", "REJ", "RSTO", "RSTR", "RSTOS0", "SH", "RSTRH", "SHR",
			"RESTR_FLAG" };
	private static final int UNKNOWN = -1;

	private static final Map<String, Integer> FLAGS_MAP = new HashMap<String, Integer>();
	{
		for (int i = 0; i < FLAG_NAMES.length; i++) {
			FLAGS_MAP.put(FLAG_NAMES[i], i);
		}
	}

	public Integer call(String flagField) {
		if (!FLAGS_MAP.containsKey(flagField)) {
			log.warn("Unknown flag field value: " + flagField);
			return UNKNOWN;
		}
		return FLAGS_MAP.get(flagField);
	}
}
