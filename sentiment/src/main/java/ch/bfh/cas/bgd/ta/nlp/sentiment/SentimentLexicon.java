package ch.bfh.cas.bgd.ta.nlp.sentiment;

import java.io.IOException;
import java.util.HashSet;

import ch.bfh.cas.bgd.ta.spark.Configuration;
import ch.bfh.cas.bgd.ta.util.TextFile;
import scala.Serializable;

public class SentimentLexicon implements Serializable {

	private static final long serialVersionUID = 834567546718064765L;

	private HashSet<String> negatives;
	private HashSet<String> positives;
	
	public SentimentLexicon() throws IOException {
		negatives = TextFile.readFileAsHashSet(Configuration.NEGATIVE_WORDS_LOCAL);
		positives = TextFile.readFileAsHashSet(Configuration.POSITIVE_WORDS_LOCAL);			
	}
	
	public boolean hasNegativeSentiment(String word) {
		if (word == null) return false;
		return (negatives.contains(word));
	}
	
	public boolean hasPositiveSentiment(String word) {
		if (word == null) return false;
		return (positives.contains(word));
	}	
	
	public static void main(String[] args) throws IOException {
		SentimentLexicon sl = new SentimentLexicon();
		System.out.println(sl.hasPositiveSentiment("hate"));
		System.out.println(sl.hasNegativeSentiment("hate"));
	}	
}
