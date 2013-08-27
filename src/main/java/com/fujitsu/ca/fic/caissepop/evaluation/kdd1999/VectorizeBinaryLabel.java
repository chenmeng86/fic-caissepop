package com.fujitsu.ca.fic.caissepop.evaluation.kdd1999;

import datafu.pig.util.SimpleEvalFunc;

public class VectorizeBinaryLabel extends SimpleEvalFunc<Integer> {
    private static final String NORMAL_TYPE = "normal";

    public Integer call(String label) {
	return label.equalsIgnoreCase(NORMAL_TYPE) ? 1 : 0;
    }
}
