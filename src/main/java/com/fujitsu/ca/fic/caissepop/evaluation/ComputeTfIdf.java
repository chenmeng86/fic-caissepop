package com.fujitsu.ca.fic.caissepop.evaluation;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fujitsu.ca.fic.caissepop.util.Util;

/**
 * Pig UDF that will compute the TF-IDF value from the term frequency, document frequency and document count.
 * 
 * @author dumoulma
 * 
 */
public class ComputeTfIdf extends EvalFunc<Double> {
    private final Logger LOG = LoggerFactory.getLogger(ComputeTfIdf.class);

    @Override
    public Double exec(Tuple input) throws IOException {
        if (input == null || input.size() != 3) {
            LOG.warn("ComputeTfIdf: input size is != 3 or input is null!");
            return null;
        }
        if (input.isNull(0) || input.isNull(1) || input.isNull(2)) {
            LOG.warn("ComputeTfIdf: either of the input values isNull()");
            return null;
        }

        long tfCount = (Long) input.get(0);
        long nDocs = (Long) input.get(1);
        long dfCount = (Long) input.get(2);

        // (double) tf_count * LOG( (double) dPipe.n_docs / ( 1.0 + (double) df_count ) ) AS tfidf
        // Formula: tf * log ( nDocs/df)
        double tfidfScore = tfCount * Math.log(nDocs / (1.0 + dfCount));
        LOG.info(String.format("tfCount: %d nDocs: %d dfCount: %d", tfCount, nDocs, dfCount));

        return Util.round(tfidfScore, 5);
    }
}
