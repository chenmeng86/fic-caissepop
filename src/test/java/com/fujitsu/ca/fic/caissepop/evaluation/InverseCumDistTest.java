package com.fujitsu.ca.fic.caissepop.evaluation;

import java.io.IOException;

import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Test;

public class InverseCumDistTest {
    @Test
    public void testGivenTPHalfOfPositivesShouldReturnZero() throws IOException, ParseException {
        String[] params = {"INPUT=data/test/icd-1.txt", "OUTPUT=data/out/test/icd"};
        PigTest pigTest = new PigTest("pig/icd-test.pig", params);
        String[] expected = {"(0.0)"};
        pigTest.assertOutput("out", expected);
    }
}
