package com.fujitsu.ca.fic.caissepop.evaluation;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Test;

public class InverseCumDistTest {
	private static final double LIMIT_ICD = 3.29053;

	@Test
	public void givenTPHalfOfPositivesShouldReturnZero() throws IOException,
			ParseException {
		String[] params = { "INPUT=data/test/icd-1.txt",
				"OUTPUT=data/out/test/icd" };
		PigTest pigTest = new PigTest("pig/icd-test.pig", params);
		String[] expected = { "(0.0)" };
		pigTest.assertOutput("out", expected);
	}

	@Test(expected = IOException.class)
	public void givenTPgreaterThanPosShouldThrowException() throws IOException {
		InverseCumDist vectorizer = new InverseCumDist();
		Tuple input = TupleFactory.getInstance().newTuple();
		input.append(100L);
		input.append(50L);

		vectorizer.exec(input);		
	}
	
	@Test
	public void givenTprGreaterThanLimitShouldComputeForHighLimit() throws IOException {
		InverseCumDist vectorizer = new InverseCumDist();
		Tuple input = TupleFactory.getInstance().newTuple();
		input.append(1L);
		input.append(10000L);

		Double icd = vectorizer.exec(input);
		assertThat(icd, equalTo(-LIMIT_ICD));
	}

	@Test
	public void givenTprSmallerThanLimitShouldComputeForLowLimit() throws IOException {
		InverseCumDist vectorizer = new InverseCumDist();
		Tuple input = TupleFactory.getInstance().newTuple();
		input.append(9999L);
		input.append(10000L);

		Double icd = vectorizer.exec(input);		
		assertThat(icd, equalTo(LIMIT_ICD));
	}
}
