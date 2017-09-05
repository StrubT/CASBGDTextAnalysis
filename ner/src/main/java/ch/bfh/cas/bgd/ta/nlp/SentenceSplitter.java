package ch.bfh.cas.bgd.ta.nlp;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import ch.bfh.cas.bgd.ta.spark.Configuration;
import opennlp.tools.sentdetect.SentenceDetector;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.util.InvalidFormatException;
import scala.Serializable;

public class SentenceSplitter implements Serializable {
	private static final long serialVersionUID = 4656938324634122467L;
	
	private SentenceDetector sentenceDetector = null;
	
	public SentenceSplitter() throws InvalidFormatException, IOException {
		InputStream sentenceModelInputStream = new FileInputStream(new File(Configuration.SENTENCE_MODEL));
		SentenceModel sentenceModel = new SentenceModel(sentenceModelInputStream);
		sentenceDetector = new SentenceDetectorME(sentenceModel);		
	}
	
	public List<String> split(String text) {
		List<String> sentences = new ArrayList<String>();
		String[] s = sentenceDetector.sentDetect(text);
		for (int i = 0; i < s.length; ++i) {
			sentences.add(s[i]);
		}
		return sentences;
	}
}
