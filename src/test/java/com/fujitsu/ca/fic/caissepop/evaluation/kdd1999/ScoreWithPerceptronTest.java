package com.fujitsu.ca.fic.caissepop.evaluation.kdd1999;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.math.Vector;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.fujitsu.ca.fic.classifier.perceptron.Perceptron;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

import static org.hamcrest.MatcherAssert.assertThat;

import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.hamcrest.number.OrderingComparison.greaterThanOrEqualTo;

import static org.mockito.Matchers.anyObject;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ScoreWithPerceptronTest {
    private static final int FIELDS = 42;
    private final String goodLineOfData = "1,0,1,12,1,146,38132,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,2,20,0.0,0.0,0.0,0.0,1.0,0.0,0.2,255.0,255.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0";
    private final String lineOfDataWithIncorrectNumberOfFields = "0,3,22,1,1032,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,511,511,0.0,0.0,0.0,0.0,1.0,0.0,0.0,255.0,255.0,1.0,0.0,1.0,0.0,0.0,0.0,0.0,0.0";

    private static final Configuration conf = new Configuration();

    @Mock
    Perceptron perceptronMock;

    @BeforeClass
    public static void beforeClass() {
        conf.set("perceptron.model.path", "data/test/model/perceptron.model");
    }

    @Test
    public void testExecWithMockPerceptron() throws Exception {
        TupleFactory tupleFactory = TupleFactory.getInstance();
        ScoreWithPerceptron scorer = new ScoreWithPerceptron();

        when(perceptronMock.classifyScalar((Vector) anyObject())).thenReturn(new Double(1.0));
        scorer.setPerceptron(perceptronMock);
        Tuple testTuple = tupleFactory.newTuple(toDoubleList(goodLineOfData));

        assertThat(testTuple.size(), equalTo(FIELDS));

        Double confidenceScore = scorer.exec(testTuple);

        assertThat(confidenceScore, greaterThan(0.0));
    }

    @Test
    public void testExecWithPerceptronReturnsDouble() throws IOException {
        TupleFactory tupleFactory = TupleFactory.getInstance();
        ScoreWithPerceptron scorer = new ScoreWithPerceptron();
        scorer.setConf(conf);

        List<Double> list = toDoubleList(goodLineOfData);
        Tuple testTuple = tupleFactory.newTuple(list);
        assertThat(testTuple.size(), equalTo(FIELDS));

        Double confidenceScore = scorer.exec(testTuple);

        assertThat(confidenceScore, greaterThanOrEqualTo(new Double(0.0)));
    }

    @Test
    public void testWithIncorrectlyFormattedDataShouldThrow() throws IOException {
        TupleFactory tupleFactory = TupleFactory.getInstance();
        ScoreWithPerceptron scorer = new ScoreWithPerceptron();
        scorer.setConf(conf);

        Tuple testTuple = tupleFactory.newTuple(toDoubleList(lineOfDataWithIncorrectNumberOfFields));
        scorer.exec(testTuple);
    }

    @Test
    public void testPerceptronModelLoadsFromConf() throws IOException {
        Perceptron p = new Perceptron(conf);
        System.out.println(p);

        assertThat(p.toString().contains("alpha lengthSquared=300.0"), is(true));
    }

    private List<Double> toDoubleList(String line) {
        String[] tokens = line.split(",");
        return Lists.transform(Arrays.asList(tokens), new Function<String, Double>() {
            @Override
            public Double apply(String token) {
                return Double.parseDouble(token);
            }
        });
    }
}
