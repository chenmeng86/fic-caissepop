package com.fujitsu.ca.fic.caissepop.evaluation.kdd1999;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

public class VectorizeFlag extends EvalFunc<Integer> {
    static final String SF_FLAG = "SF";
    static final String S0_FLAG = "S0";
    static final String S1_FLAG = "S1";
    static final String S2_FLAG = "S2";
    static final String S3_FLAG = "S3";
    static final String OTH_FLAG = "OTH";
    static final String REJ_FLAG = "REJ";
    static final String RSTO_FLAG = "RSTO";
    static final String RSTR_FLAG = "RSTR";
    static final String RSTOS0_FLAG = "RSTOS0";
    static final String SH_FLAG = "SH";
    static final String RSTRH_FLAG = "RSTRH";
    static final String SHR_FLAG = "SHR";
    static final String RESTR_FLAG = "RESTR";

    @Override
    public Integer exec(Tuple input) throws IOException {
        if (input == null || input.size() != 1 || input.isNull(0))
            return null;

        String flagType = (String) input.get(0);
        int flagTypeValue = -1;
        if (flagType.equalsIgnoreCase(SF_FLAG))
            flagTypeValue = 1;
        else if (flagType.equalsIgnoreCase(S0_FLAG))
            flagTypeValue = 2;
        else if (flagType.equalsIgnoreCase(S1_FLAG))
            flagTypeValue = 3;
        else if (flagType.equalsIgnoreCase(S2_FLAG))
            flagTypeValue = 4;
        else if (flagType.equalsIgnoreCase(S3_FLAG))
            flagTypeValue = 5;
        else if (flagType.equalsIgnoreCase(OTH_FLAG))
            flagTypeValue = 6;
        else if (flagType.equalsIgnoreCase(REJ_FLAG))
            flagTypeValue = 7;
        else if (flagType.equalsIgnoreCase(RSTO_FLAG))
            flagTypeValue = 8;
        else if (flagType.equalsIgnoreCase(RSTR_FLAG))
            flagTypeValue = 9;
        else if (flagType.equalsIgnoreCase(SH_FLAG))
            flagTypeValue = 10;
        else if (flagType.equalsIgnoreCase(RSTRH_FLAG))
            flagTypeValue = 11; // doesnt happen?
        else if (flagType.equalsIgnoreCase(SHR_FLAG))
            flagTypeValue = 12; // doesnt happen?
        else if (flagType.equalsIgnoreCase(RSTOS0_FLAG))
            flagTypeValue = 13;
        else {
            throw new ExecException("Flag Type unknown value: " + flagType, PigException.INPUT);
        }
        return flagTypeValue;
    }
}
