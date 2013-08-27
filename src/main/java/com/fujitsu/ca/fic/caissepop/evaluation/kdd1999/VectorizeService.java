package com.fujitsu.ca.fic.caissepop.evaluation.kdd1999;

import java.util.HashMap;
import java.util.Map;
import org.apache.pig.PigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datafu.pig.util.SimpleEvalFunc;

public class VectorizeService extends SimpleEvalFunc<Integer> {
    private static final int UNKNOWN_FLAG = -1;
    private static final Logger LOG = LoggerFactory.getLogger(VectorizeService.class);
    private static final String[] SERVICES_NAMES = { "bgp", "ctf", "efs", "ftp", "irc", "mtp",
	    "rje", "ssh", "x11", "auth", "echo", "exec", "http", "ldap", "link", "name", "nnsp",
	    "nntp", "smtp", "time", "uucp", "eco_i", "ecr_i", "imap4", "login", "ntp_u", "other",
	    "pop_2", "pop_3", "red_i", "shell", "tim_i", "urh_i", "urp_i", "vmnet", "whois",
	    "domain", "finger", "gopher", "klogin", "kshell", "sunrpc", "supdup", "systat",
	    "telnet", "tftp_u", "z39_50", "courier", "daytime", "discard", "netstat", "pm_dump",
	    "printer", "private", "sql_net", "csnet_ns", "domain_u", "ftp_data", "http_443",
	    "iso_tsap", "hostnames", "uucp_path", "netbios_ns", "remote_job", "netbios_dgm",
	    "netbios_ssn" };

    private static final Map<String, Integer> SERVICES_MAP = new HashMap<String, Integer>();
    {
	for (int i = 0; i < SERVICES_NAMES.length; i++) {
	    SERVICES_MAP.put(SERVICES_NAMES[i], i);
	}
    }

    public Integer call(String serviceString) {
	if (!SERVICES_MAP.containsKey(serviceString)) {
	    LOG.warn("Service Type unknown value: " + serviceString, PigException.INPUT);
	    return UNKNOWN_FLAG;
	}
	return SERVICES_MAP.get(serviceString);
    }
}
