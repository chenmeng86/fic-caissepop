package com.fujitsu.ca.fic.caissepop.store;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VectorizedDocumentStorer extends StoreFunc {
    Logger LOG = LoggerFactory.getLogger(VectorizedDocumentStorer.class);
    private RecordWriter<Text, VectorWritable> writer;
    private final int cardinality;

    public VectorizedDocumentStorer(int cardinality) {
        this.cardinality = cardinality;
    }

    @Override
    public OutputFormat<Text, VectorWritable> getOutputFormat() throws IOException {
        return new SequenceFileOutputFormat<Text, VectorWritable>();
    }

    @Override
    public void checkSchema(ResourceSchema s) throws IOException {
        super.checkSchema(s);
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        // TODO Auto-generated method stub

    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void prepareToWrite(RecordWriter writer) throws IOException {
        this.writer = writer;
    }

    static long index = 0;

    @Override
    public void putNext(Tuple t) throws IOException {
        // get the label and document name
        String documentLabel = "";
        String documentName = "";
        Vector vector = new SequentialAccessSparseVector(cardinality);
        double[] features = new double[cardinality];

        // go through the bag of scoredFeature tuples
        // for each tuple of the bag of features
        vector.assign(features);
        LOG.debug(String.format("Vector: label:%s Fields: %d", documentLabel, vector.size()));

        NamedVector nextVector = new NamedVector(vector, documentLabel);
        try {
            writer.write(new Text(documentName), new VectorWritable(nextVector));
        } catch (InterruptedException ie) {
            throw new IOException(ie);
        }
    }
}
