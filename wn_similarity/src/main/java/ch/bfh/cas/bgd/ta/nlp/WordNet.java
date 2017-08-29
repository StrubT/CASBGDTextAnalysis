
package ch.bfh.cas.bgd.ta.nlp;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import ch.bfh.cas.bgd.ta.spark.Configuration;
import net.didion.jwnl.JWNL;
import net.didion.jwnl.JWNLException;
import net.didion.jwnl.data.IndexWord;
import net.didion.jwnl.data.POS;
import net.didion.jwnl.data.Synset;
import net.didion.jwnl.data.Word;
import net.didion.jwnl.dictionary.Dictionary;
import scala.Serializable;

public class WordNet implements Serializable {

	private static final long serialVersionUID = -584075437666472001L;

	private static Logger logger = LogManager.getLogger(WordNet.class);

	private static Dictionary wordnetDictionary = null;

	private HashMap<String, HashSet<String>> synonymCache = new HashMap<>();

	public WordNet() throws FileNotFoundException, JWNLException {

		if (wordnetDictionary == null) {
			// prepare wordnet dictionary
			JWNL.initialize(new FileInputStream(Configuration.WORDNET_PROPERTIES));
			wordnetDictionary = Dictionary.getInstance();
		}
	}

	public HashSet<String> getSynonyms(String word) throws JWNLException {

		HashSet<String> synonyms = synonymCache.get(word);
		if (synonyms != null) {
			logger.info("found " + synonyms.size() + " cached synonyms for " + word + ": " + synonyms.toString());
			return synonyms;
		}

		synonyms = new HashSet<>();
		List<Synset> synsets = getSynsetsFromWN(word);
		Iterator<Synset> i = synsets.iterator();
		while (i.hasNext()) {
			Word[] synWords = i.next().getWords();
			for (Word synWord : synWords)
				synonyms.add(synWord.getLemma());
		}

		synonymCache.put(word, synonyms);

		logger.info("found " + synonyms.size() + " synonyms for " + word + ": " + synonyms.toString());
		return synonyms;
	}

	private List<Synset> getSynsetsFromWN(String word) throws JWNLException {

		List<Synset> list = new ArrayList<>();
		List<IndexWord> iwList = getIndexWordsFromWN(word);
		Iterator<IndexWord> i = iwList.iterator();
		while (i.hasNext()) {
			Synset[] synsets = i.next().getSenses();
			for (Synset synset : synsets)
				list.add(synset);
		}
		return list;
	}

	private List<IndexWord> getIndexWordsFromWN(String word) throws JWNLException {

		List<IndexWord> list = new ArrayList<>();
		// we do not use a POS tagger so we try all possibilities
		IndexWord iw = wordnetDictionary.getIndexWord(POS.ADJECTIVE, word);
		if (iw != null) list.add(iw);
		iw = wordnetDictionary.getIndexWord(POS.ADVERB, word);
		if (iw != null) list.add(iw);
		iw = wordnetDictionary.getIndexWord(POS.NOUN, word);
		if (iw != null) list.add(iw);
		iw = wordnetDictionary.getIndexWord(POS.VERB, word);
		if (iw != null) list.add(iw);
		return list;
	}
}
