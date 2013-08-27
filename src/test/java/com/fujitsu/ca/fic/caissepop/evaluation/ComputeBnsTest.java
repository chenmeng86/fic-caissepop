package com.fujitsu.ca.fic.caissepop.evaluation;

import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;

import static org.hamcrest.MatcherAssert.assertThat;

import static org.hamcrest.number.OrderingComparison.greaterThan;

public class ComputeBnsTest {
    private final ComputeBns bnsClass = new ComputeBns();

    @Test
    public void testGivenEqualTpAndTnCountsShouldReturnZero() throws IOException, ParseException {
        String[] params = { "INPUT=data/test/bns-1.txt", "OUTPUT=data/out/test/bns" };
        String[] expected = { "(0.0)" };

        PigTest pigTest = new PigTest("pig/compute_bns-test.pig", params);

        pigTest.assertOutput("out", expected);
    }

    @Test
    public void testGiven10CountsShouldReturnCorrect10BnsValues() throws IOException,
            ParseException {
        String[] params = { "INPUT=data/test/bns-10.txt", "OUTPUT=data/out/test/bns2" };
        String[] expected = { "(6.58105)", "(2.5631)", "(1.68324)", "(1.0488)", "(0.50669)",
                "(0.0)", "(0.50669)", "(1.0488)", "(1.68324)", "(2.5631)", "(6.58105)" };

        PigTest pigTest = new PigTest("pig/compute_bns-test.pig", params);

        pigTest.assertOutput("out", expected);
    }

    @Test
    public void givenHighTPRShouldReturnHighScore() throws IOException {
        Tuple input = TupleFactory.getInstance().newTuple();
        input.append(8L);
        input.append(10L);
        input.append(0L);
        input.append(10L);

        Double bnsScore = bnsClass.exec(input);

        assertThat(bnsScore, greaterThan(4.0));
    }

    @Test
    public void givenHighTNRShouldReturnHighScore() throws IOException {
        Tuple input = TupleFactory.getInstance().newTuple();
        input.append(0L);
        input.append(10L);
        input.append(8L);
        input.append(10L);

        Double bnsScore = bnsClass.exec(input);

        assertThat(bnsScore, greaterThan(4.0));
    }

    @Test
    public void givenBalancedRatioShouldReturnZero() throws IOException {
        Tuple input = TupleFactory.getInstance().newTuple();
        input.append(5L);
        input.append(10L);
        input.append(5L);
        input.append(10L);

        Double bnsScore = bnsClass.exec(input);

        assertThat(bnsScore, equalTo(0.0));
    }
}
