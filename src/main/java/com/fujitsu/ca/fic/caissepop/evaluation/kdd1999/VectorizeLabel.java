package com.fujitsu.ca.fic.caissepop.evaluation.kdd1999;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VectorizeLabel extends EvalFunc<Integer> {
    private final Logger log = LoggerFactory.getLogger(VectorizeLabel.class);
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
        int labelValue = -1;
        if (label.equalsIgnoreCase(NORMAL_TYPE)) {
            labelValue = 1;
        } else if (isDosAttackType(label)) {
            labelValue = 1;
        } else if (isProbeAttackType(label)) {
            labelValue = 2;
        } else if (isU2RAttackType(label)) {
            labelValue = 3;
        } else if (isR2LAttackType(label)) {
            labelValue = 4;
        } else {
            throw new ExecException("Label has unknown value: " + label, PigException.INPUT);
        }
        return labelValue;
    }

    private boolean isDosAttackType(String label) {
        return label.equalsIgnoreCase("back") || label.equalsIgnoreCase("land") || label.equalsIgnoreCase("neptune")
                || label.equalsIgnoreCase("pod") || label.equalsIgnoreCase("teardrop") || label.equalsIgnoreCase("smurf");
    }

    private boolean isProbeAttackType(String label) {
        return label.equalsIgnoreCase("ipsweep") || label.equalsIgnoreCase("nmap") || label.equalsIgnoreCase("portsweep")
                || label.equalsIgnoreCase("satan");
    }

    private boolean isU2RAttackType(String label) {
        return label.equalsIgnoreCase("buffer_overflow") || label.equalsIgnoreCase("loadmodule") || label.equalsIgnoreCase("perl")
                || label.equalsIgnoreCase("rootkit");
    }

    private boolean isR2LAttackType(String label) {
        return label.equalsIgnoreCase("ftp_write") || label.equalsIgnoreCase("guess_passwd") || label.equalsIgnoreCase("imap")
                || label.equalsIgnoreCase("multihop") || label.equalsIgnoreCase("phf") || label.equalsIgnoreCase("spy")
                || label.equalsIgnoreCase("warezclient") || label.equalsIgnoreCase("warezmaster");
    }
}
