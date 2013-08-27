package com.fujitsu.ca.fic.caissepop.evaluation.kdd1999;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fujitsu.ca.fic.classifier.perceptron.Perceptron;

public class ScoreWithPerceptron extends EvalFunc<Double> {
    private static final Logger LOG = LoggerFactory.getLogger(ScoreWithPerceptron.class);

    private Perceptron perceptron;
    private static final int FIELDS = 42;
    private static final int FEATURES = 41;
    private static final int FIRST_FEATURE_INDEX = 1;
    private static final int LABEL_INDEX = 0;

    @Override
    public Double exec(Tuple input) throws IOException {
        if (input == null) {
            LOG.warn("input is null!");
            return null;
        }
        if (input.size() != FIELDS) {
            LOG.warn("Unexpected input Tuple size: " + input.size());
            return null;
        }

        // lazy loading of the percepton
        if (perceptron == null) {
            loadPerceptronFromFS();
        }

        NamedVector example = tupleToNamedVector(input);
        return perceptron.classifyScalar(example);
    }

    private NamedVector tupleToNamedVector(Tuple input) throws ExecException {
        double[] featuresDouble = new double[FEATURES];

        int label = -1;
        if (input.getType(LABEL_INDEX) == DataType.INTEGER) {
            label = (Integer) input.get(LABEL_INDEX);
        } else if (input.getType(LABEL_INDEX) == DataType.DOUBLE) {
            label = ((Double) input.get(LABEL_INDEX)).intValue();
        } else {
            throw new ExecException("Unexpected format for label field: " + input.get(LABEL_INDEX));
        }

        int j = 0;
        for (int i = FIRST_FEATURE_INDEX; i < FIELDS; i++) {
            if (input.getType(i) == DataType.INTEGER) {
                featuresDouble[j++] = (Integer) input.get(i);
            } else if (input.getType(i) == DataType.DOUBLE) {
                featuresDouble[j++] = (Double) input.get(i);
            } else {
                throw new ExecException("Unexpected format for field: " + i + "=" + input.get(i));
            }
        }

        Vector featureVector = new SequentialAccessSparseVector(FEATURES);
        featureVector.assign(featuresDouble);

        return new NamedVector(featureVector, Integer.toString(label));
    }

    private void loadPerceptronFromFS() throws IOException {
        perceptron = new Perceptron(UDFContext.getUDFContext().getJobConf());
    }

    // FOR TESTING PURPOSES
    void setPerceptron(Perceptron perceptronMock) {
        perceptron = perceptronMock;
    }

    // FOR TESTING PURPOSES
    void setConf(Configuration conf) {
        UDFContext.getUDFContext().addJobConf(conf);
    }
}
