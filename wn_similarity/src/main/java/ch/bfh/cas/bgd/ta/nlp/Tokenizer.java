
package ch.bfh.cas.bgd.ta.nlp;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import ch.bfh.cas.bgd.ta.spark.Configuration;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import scala.Serializable;

public class Tokenizer implements Serializable {

	private static final long serialVersionUID = 3586744315181140599L;

	private static Logger logger = LogManager.getLogger(Tokenizer.class);

	// private static opennlp.tools.tokenize.Tokenizer tokenizer = null;
	// interestingly, a singleton generate an NPE for more than 1 node in spark standalone mode
	private transient opennlp.tools.tokenize.Tokenizer tokenizer = null;

	public List<String> tokenize(String text) throws IOException {

		if (text == null || text.isEmpty()) return new ArrayList<>(); // return empty list

		if (tokenizer == null) //
			try (InputStream modelInputStream = new FileInputStream(new File(Configuration.TOKENIZER_MODEL))) {
			TokenizerModel model = new TokenizerModel(modelInputStream);
			tokenizer = new TokenizerME(model);
			}

		List<String> tokens = new ArrayList<>(Arrays.asList(tokenizer.tokenize(text)));
		return tokens;
	}
}
