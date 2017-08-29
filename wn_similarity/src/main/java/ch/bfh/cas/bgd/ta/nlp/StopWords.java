package ch.bfh.cas.bgd.ta.nlp;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import ch.bfh.cas.bgd.ta.spark.Configuration;
import ch.bfh.cas.bgd.ta.util.TextFile;
import scala.Serializable;

public class StopWords implements Serializable {
	private static final long serialVersionUID = -3081663862830139354L;

	private static Logger logger = LogManager.getLogger(StopWords.class);
	
	private static HashSet<String> stopwords = null;
	
	public StopWords() throws IOException {
		if (stopwords == null) {
			stopwords = new HashSet<String>();

			// read stopwords from file
			String text = TextFile.readFile(Configuration.STOPWORDS_FILE);
			StringTokenizer st = new StringTokenizer(text);
			while (st.hasMoreTokens()) {
				String term = st.nextToken();
				stopwords.add(term);
			}
			logger.debug(
					"Stopwords read from file " + Configuration.STOPWORDS_FILE + " containing " + stopwords.size() + " stopwords");
		}
	}

	public boolean isStopword(String term) {
		if (stopwords == null) 
			return false; // error
		
		if (stopwords.contains(term)) 
			return true; // found it
		
		// not found
		return false;
	}
}
