package com.fujitsu.ca.fic.caissepop.evaluation.kdd1999;

import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;

import static org.hamcrest.MatcherAssert.assertThat;

public class VectorizeProtocolTypeTest {
    @Test
    public void givenTokenShouldReturnCorrectNumber1() throws IOException {
        VectorizeProtocolType vectorizer = new VectorizeProtocolType();
        Tuple input = TupleFactory.getInstance().newTuple();
        input.append("tcp");

        int value = vectorizer.exec(input);
        assertThat(value, equalTo(1));
    }

    @Test
    public void givenTokenShouldReturnCorrectNumber2() throws IOException {
        VectorizeProtocolType vectorizer = new VectorizeProtocolType();
        Tuple input = TupleFactory.getInstance().newTuple();
        input.append("udp");

        int value = vectorizer.exec(input);
        assertThat(value, equalTo(2));
    }

    @Test
    public void givenTokenShouldReturnCorrectNumber3() throws IOException {
        VectorizeProtocolType vectorizer = new VectorizeProtocolType();
        Tuple input = TupleFactory.getInstance().newTuple();
        input.append("icmp");

        int value = vectorizer.exec(input);
        assertThat(value, equalTo(3));
    }
}
