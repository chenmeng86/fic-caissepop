package com.fujitsu.ca.fic.caissepop.evaluation.kdd1999;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class VectorizeBinaryLabel extends EvalFunc<Integer> {
    static final String NORMAL_TYPE = "normal";

    @Override
    public Integer exec(Tuple input) throws IOException {
        if (input == null || input.size() != 1 || input.isNull(0))
            return null;

        String label = (String) input.get(0);
        return label.equalsIgnoreCase(NORMAL_TYPE) ? 1 : 0;
    }
}
