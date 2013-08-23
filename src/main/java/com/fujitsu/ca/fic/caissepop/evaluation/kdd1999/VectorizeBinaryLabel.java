package com.fujitsu.ca.fic.caissepop.evaluation.kdd1999;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VectorizeBinaryLabel extends EvalFunc<Integer> {
    private final Logger log = LoggerFactory.getLogger(VectorizeBinaryLabel.class);
    private static final String NORMAL_TYPE = "normal";

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

        String label = (String) input.get(0);
        return label.equalsIgnoreCase(NORMAL_TYPE) ? 1 : 0;
    }
}
