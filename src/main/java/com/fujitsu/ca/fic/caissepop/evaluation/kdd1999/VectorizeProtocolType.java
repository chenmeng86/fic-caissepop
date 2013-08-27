package com.fujitsu.ca.fic.caissepop.evaluation.kdd1999;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VectorizeProtocolType extends EvalFunc<Integer> {
    private static final Logger LOG = LoggerFactory.getLogger(VectorizeProtocolType.class);
    private static final String TCP_TYPE = "tcp";
    private static final String UDP_TYPE = "udp";
    private static final String ICMP_TYPC = "icmp";

    @Override
    public Integer exec(Tuple input) throws IOException {
        if (input == null) {
            LOG.warn("input is null!");
            return null;
        }
        if (input.size() != 1 || input.isNull(0)) {
            LOG.warn("Unexpected input Tuple size: " + input.size());
            return null;
        }

        String protocolType = (String) input.get(0);
        int protocolTypeInt = -1;
        if (protocolType.equalsIgnoreCase(TCP_TYPE)) {
            protocolTypeInt = 1;
        } else if (protocolType.equalsIgnoreCase(UDP_TYPE)) {
            protocolTypeInt = 2;
        } else if (protocolType.equalsIgnoreCase(ICMP_TYPC)) {
            protocolTypeInt = 3;
        } else {
            throw new ExecException("Protocol Type unknown value: " + protocolType, PigException.INPUT);
        }
        return protocolTypeInt;
    }

}
