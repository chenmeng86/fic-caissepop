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

public class VectorizeService extends EvalFunc<Integer> {
	private final Logger log = LoggerFactory.getLogger(VectorizeService.class);
	private static final String[] SERVICES_NAMES = { "bgp", "ctf", "efs",
			"ftp", "irc", "mtp", "rje", "ssh", "x11", "auth", "echo", "exec",
			"http", "ldap", "link", "name", "nnsp", "nntp", "smtp", "time",
			"uucp", "eco_i", "ecr_i", "imap4", "login", "ntp_u", "other",
			"pop_2", "pop_3", "red_i", "shell", "tim_i", "urh_i", "urp_i",
			"vmnet", "whois", "domain", "finger", "gopher", "klogin", "kshell",
			"sunrpc", "supdup", "systat", "telnet", "tftp_u", "z39_50",
			"courier", "daytime", "discard", "netstat", "pm_dump", "printer",
			"private", "sql_net", "csnet_ns", "domain_u", "ftp_data",
			"http_443", "iso_tsap", "hostnames", "uucp_path", "netbios_ns",
			"remote_job", "netbios_dgm", "netbios_ssn" };

	private final Map<String, Integer> services = new HashMap<String, Integer>();

	public VectorizeService() {
		for (int i = 0; i < SERVICES_NAMES.length; i++) {
			services.put(SERVICES_NAMES[i], i);
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

		String serviceString = (String) input.get(0);
		if (!services.containsKey(serviceString)) {
			throw new ExecException("Service Type unknown value: "
					+ serviceString, PigException.INPUT);
		}
		return services.get(serviceString);
	}
}
