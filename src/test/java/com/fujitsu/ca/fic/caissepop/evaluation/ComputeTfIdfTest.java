package com.fujitsu.ca.fic.caissepop.evaluation;

import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;

import static org.hamcrest.number.IsCloseTo.closeTo;

public class ComputeTfIdfTest {
    @Test
    public void testGivenSomeCountsShouldReturnTfIdfValue() throws IOException, ParseException {
        String[] params = { "INPUT=data/test/tfidf-1.txt", "OUTPUT=data/out/test/tfidf" };
        String[] expected = { "(6.01986)" };

        PigTest pigTest = new PigTest("pig/compute_tfidf-test.pig", params);

        pigTest.assertOutput("out", expected);
    }

    @Test
    public void testGivenTfCountZeroShouldReturnZero() throws IOException, ParseException {
        String[] params = { "INPUT=data/test/tfidf-2.txt", "OUTPUT=data/out/test/tfidf2" };
        String[] expected = { "(0.0)" };

        PigTest pigTest = new PigTest("pig/compute_tfidf-test.pig", params);

        pigTest.assertOutput("out", expected);
    }

    @Test
    public void testEvalFunc() throws IOException {
        final double errorMargin = 0.2;
        ComputeTfIdf udfClass = new ComputeTfIdf();
        Tuple input = TupleFactory.getInstance().newTuple();
        input.append(5L);
        input.append(10L);
        input.append(2L);

        Double tfidf = udfClass.exec(input);

        assertThat(tfidf, closeTo(6.0, errorMargin));
    }
}
