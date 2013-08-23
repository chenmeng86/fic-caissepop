package com.fujitsu.ca.fic.classifier.perceptron;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.classifier.AbstractVectorClassifier;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;

public class Perceptron extends AbstractVectorClassifier {
    private static final String PERCEPTRON_MODEL_CONF_NAME = "perceptron.model.path";

    private final RBFKernel kernel;

    private Matrix positiveDataset;
    private Vector alpha;
    private double gamma;

    @Override
    public double classifyScalar(Vector example) {
        double sums = 0.0;

        for (int j = 0; j < positiveDataset.numRows(); j++) {
            sums += alpha.get(j) * kernel.calculateScalarProduct(positiveDataset.viewRow(j), example);
        }
        return sums;
    }

    @Override
    public int numCategories() {
        return 2;
    }

    @Override
    public Vector classify(Vector example) {
        // Will never happen, since a perceptron is a binary classificator.
        // Nevertheless, implements classifyScalar with a Vector as the return
        // value.
        Vector classification = new DenseVector(1);

        classification.set(0, classifyScalar(example));
        return classification;
    }

    public Perceptron(Configuration conf) throws IOException {
        FSDataInputStream input = null;

        try {
            String perceptronModelPath = conf.get(PERCEPTRON_MODEL_CONF_NAME);
            FileSystem fs = FileSystem.get(conf);
            input = fs.open(new Path(perceptronModelPath));

            loadFromFile(input);
        } finally {
            if (input != null) {
                input.close();
            }
        }

        kernel = new RBFKernel(gamma);
    }

    private void loadFromFile(DataInputStream input) throws IOException {
        gamma = input.readDouble();

        int alphaSize = input.readInt();
        alpha = new DenseVector(alphaSize);
        for (int row = 0; row < alpha.size(); row++) {
            alpha.set(row, input.readDouble());
        }

        int rowSize = input.readInt();
        int colSize = input.readInt();
        positiveDataset = new DenseMatrix(rowSize, colSize);
        for (int row = 0; row < positiveDataset.numRows(); row++) {
            for (int col = 0; col < positiveDataset.numCols(); col++) {
                positiveDataset.set(row, col, input.readDouble());
            }
        }
    }

    public void saveToFile(Configuration conf) throws IOException {
        FSDataOutputStream output = null;
        Path modelPath = new Path(conf.get(PERCEPTRON_MODEL_CONF_NAME));

        try {
            HadoopUtil.delete(conf, modelPath);
            FileSystem fs = FileSystem.get(conf);
            output = fs.create(modelPath);

            output.writeDouble(gamma);
            output.writeInt(alpha.size());
            for (int row = 0; row < alpha.size(); row++) {
                output.writeDouble(alpha.get(row));
            }
            output.writeInt(positiveDataset.numRows());
            output.writeInt(positiveDataset.numCols());
            for (int row = 0; row < positiveDataset.numRows(); row++) {
                for (int col = 0; col < positiveDataset.numCols(); col++) {
                    output.writeDouble(positiveDataset.get(row, col));
                }
            }
        } finally {
            if (output != null) {
                output.close();
            }
        }
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("alpha lengthSquared=" + alpha.getLengthSquared() + "\n");
        sb.append("alpha: ");
        for (int i = 0; i < alpha.size(); i++) {
            Element element = alpha.getElement(i);
            if (element.get() != 0.0) {
                sb.append(i + ":" + element.get() + ", ");
            }
        }
        sb.append("\n");
        sb.append("Gamma=" + gamma + "\n");
        return sb.toString();
    }
}
