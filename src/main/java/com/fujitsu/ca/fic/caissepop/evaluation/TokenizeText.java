/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.fujitsu.ca.fic.caissepop.evaluation;

import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * TokenizeText uses the Lucene libraries EnglishAnalyzer class to tokenize a raw text input.
 * 
 * <dt><b>Example:</b></dt> <dd><code>
 * register varaha.jar;<br/>
 * documents    = LOAD 'documents' AS (doc_id:chararray, text:chararray);<br/>
 * tokenized    = FOREACH documents GENERATE doc_id AS doc_id, FLATTEN(TokenizeText(text)) AS (token:chararray);
 * </code></dd> </dl>
 * <p>
 * NOTE: Upgrade to Version.LUCENE_44: Must call tokenStream.reset() before tokenStream.incrementToken() or will result in
 * nullPointerException!
 * </p>
 * <i>Based on code by Jacob Perkins</i>
 * 
 * @see org.apache.lucene.analysis.en.EnglishAnalyzer
 * @author dumoulma
 */
public class TokenizeText extends EvalFunc<DataBag> {

    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    private static BagFactory bagFactory = BagFactory.getInstance();
    private static String noField = "";
    private static EnglishAnalyzer analyzer = new EnglishAnalyzer(Version.LUCENE_44);

    @Override
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() < 1 || input.isNull(0)) {
            return null;
        }

        DataBag bagOfTokens = bagFactory.newDefaultBag();
        TokenStream tokenStream = null;
        try {
            String lineOfText = input.get(0).toString();
            StringReader textInput = new StringReader(lineOfText);
            tokenStream = analyzer.tokenStream(noField, textInput);
            CharTermAttribute termAttribute = tokenStream.getAttribute(CharTermAttribute.class);
            tokenStream.reset();

            while (tokenStream.incrementToken()) {
                Tuple termText = tupleFactory.newTuple(termAttribute.toString());
                bagOfTokens.add(termText);
                termAttribute.setEmpty();
            }
        } finally {
            if (tokenStream != null) {
                tokenStream.close();
            }
        }
        return bagOfTokens;
    }
}
