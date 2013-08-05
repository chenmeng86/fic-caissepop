package com.fujitsu.ca.fic.caissepop.evaluation.kdd1999;

import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;

import static org.hamcrest.MatcherAssert.assertThat;


public class VectorizeLabelTest {
    @Test
    public void givenTokenShouldReturnCorrectNumber() throws IOException {
        VectorizeLabel vectorizer = new VectorizeLabel();
        Tuple input = TupleFactory.getInstance().newTuple();
        input.append("normal");

        int value = vectorizer.exec(input);
        assertThat(value, equalTo(1));
    }
}
