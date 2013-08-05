package com.fujitsu.ca.fic.caissepop.evaluation;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class BestBook extends EvalFunc<Tuple> {

	@SuppressWarnings("unchecked")
	@Override
	public Tuple exec(Tuple input) throws IOException {
		if (input == null || input.size() < 1 || input.isNull(0)
				|| input.isNull(1))
			return null;

		Iterator<Tuple> bagReviewers = ((DataBag) input.get(0)).iterator();
		Iterator<Tuple> bagScores = ((DataBag) input.get(1)).iterator();
		int bestScore = -1;
		String bestReviewer = null;
		while (bagReviewers.hasNext() && bagScores.hasNext()) {
			String reviewerName = (String) bagReviewers.next().get(0);
			Integer score = (Integer) bagScores.next().get(0);
			if (score.intValue() > bestScore) {
				bestScore = score;
				bestReviewer = reviewerName;
			}
		}
		return TupleFactory.getInstance().newTuple(
				Arrays.asList(bestReviewer, (Integer) bestScore));
	}

}
