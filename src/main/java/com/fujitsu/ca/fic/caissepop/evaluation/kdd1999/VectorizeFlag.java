package com.fujitsu.ca.fic.caissepop.evaluation.kdd1999;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VectorizeFlag extends EvalFunc<Integer> {
	private final Logger log = LoggerFactory.getLogger(VectorizeFlag.class);
	private static final String[] FLAG_NAMES = { "SF", "S0", "S1", "S2", "S3",
			"OTH", "REJ", "RSTO", "RSTR", "RSTOS0", "SH", "RSTRH", "SHR",
			"RESTR_FLAG" };

	private static final Map<String, Integer> FLAGS_MAP = new HashMap<String, Integer>();
	{
		for (int i = 0; i < FLAG_NAMES.length; i++) {
			FLAGS_MAP.put(FLAG_NAMES[i], i);
		}
	}

	@Override
	public Integer exec(Tuple input) throws IOException {
		if (input == null) {
			log.warn("input is null!");
			return null;
		}
		if (input.size() != 1 || input.isNull(0)) {
			log.warn("Unexpected input Tuple size: " + input.size());
			return null;
		}

		String flagString = (String) input.get(0);
		if (!FLAGS_MAP.containsKey(flagString)) {
			throw new ExecException("Flag Type unknown value: " + flagString,
					PigException.INPUT);
		}
		return FLAGS_MAP.get(flagString);
	}
}
