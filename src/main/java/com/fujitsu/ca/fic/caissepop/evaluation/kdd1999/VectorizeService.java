package com.fujitsu.ca.fic.caissepop.evaluation.kdd1999;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class VectorizeService extends EvalFunc<Integer> {
    private static final String[] servicesNames = {"bgp", "ctf", "efs", "ftp", "irc", "mtp", "rje", "ssh", "x11", "auth", "echo", "exec",
            "http", "ldap", "link", "name", "nnsp", "nntp", "smtp", "time", "uucp", "eco_i", "ecr_i", "imap4", "login", "ntp_u", "other",
            "pop_2", "pop_3", "red_i", "shell", "tim_i", "urh_i", "urp_i", "vmnet", "whois", "domain", "finger", "gopher", "klogin",
            "kshell", "sunrpc", "supdup", "systat", "telnet", "tftp_u", "z39_50", "courier", "daytime", "discard", "netstat", "pm_dump",
            "printer", "private", "sql_net", "csnet_ns", "domain_u", "ftp_data", "http_443", "iso_tsap", "hostnames", "uucp_path",
            "netbios_ns", "remote_job", "netbios_dgm", "netbios_ssn"};

    private final Map<String, Integer> services = new HashMap<String, Integer>();

    public VectorizeService() {
        for (int i = 0; i < servicesNames.length; i++) {
            services.put(servicesNames[i], i);
        }
    }

    @Override
    public Integer exec(Tuple input) throws IOException {
        if (input == null || input.size() != 1 || input.isNull(0))
            return null;

        String service = (String) input.get(0);

        return services.get(service);
    }
}
