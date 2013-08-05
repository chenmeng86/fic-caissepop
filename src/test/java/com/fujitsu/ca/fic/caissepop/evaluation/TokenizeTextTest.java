package com.fujitsu.ca.fic.caissepop.evaluation;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Test;

import com.google.common.collect.Lists;

import static org.junit.Assert.assertTrue;

public class TokenizeTextTest {
    @Test
    public void testGivenDirectoryWithOneDocShouldReturnTokenizedString() throws IOException, ParseException {
        String[] params = {"INPUT=data/tok1", "OUTPUT=data/icd-out"};
        PigTest pigTest = new PigTest("pig/tokenize-test.pig", params);

        String[] expected = {"(tokenize-1.txt,test)", "(tokenize-1.txt,sentenc)"};

        pigTest.assertOutput("out", expected);
    }

    @Test
    public void givenSimpleSentenceEvalFuncShouldReturnStemmedTokensWithNoStopWords() throws IOException {
        TokenizeText tokenizeClass = new TokenizeText();
        Tuple input = TupleFactory.getInstance().newTuple();
        input.append("This is a test sentence");

        DataBag tokensBag = tokenizeClass.exec(input);
        Set<String> tokens = new HashSet<String>();

        for (Tuple item : tokensBag) {
            String token = (String) item.get(0);
            tokens.add(token);
        }

        List<String> expected = Lists.newArrayList("test", "sentenc");
        assertTrue(tokens.containsAll(expected));
    }

    @Test
    public void givenSimpleSentenceEvalFuncShouldReturnStemmedTokensWithNoStopWords2() throws IOException {
        TokenizeText tokenizeClass = new TokenizeText();
        Tuple input = TupleFactory.getInstance().newTuple();
        input.append("Alberta Environment created the Alberta Environment Support and Emergency Response Team (ASERT) in 2006.");

        DataBag tokensBag = tokenizeClass.exec(input);
        List<String> tokens = Lists.newArrayList();

        for (Tuple item : tokensBag) {
            String token = (String) item.get(0);
            tokens.add(token);
        }

        List<String> expected = Lists.newArrayList("alberta", "environ", "creat", "alberta", "environ", "support", "emerg", "respons",
                "team", "asert", "2006");
        assertTrue(tokens.containsAll(expected));
    }
}
