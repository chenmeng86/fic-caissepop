package com.fujitsu.ca.fic.caissepop.evaluation.kdd1999;

import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;

import static org.hamcrest.MatcherAssert.assertThat;

public class VectorizeServiceTest {
    @Test
    public void givenTokenShouldReturnCorrectNumber() throws IOException {
        VectorizeService vectorizer = new VectorizeService();
        Tuple input = TupleFactory.getInstance().newTuple();
        input.append("http");

        int value = vectorizer.exec(input);
        assertThat(value, equalTo(12));
    }
}