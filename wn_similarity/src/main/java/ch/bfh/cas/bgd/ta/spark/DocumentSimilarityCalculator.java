
package ch.bfh.cas.bgd.ta.spark;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import ch.bfh.cas.bgd.ta.nlp.StopWords;
import ch.bfh.cas.bgd.ta.nlp.Tokenizer;
import ch.bfh.cas.bgd.ta.nlp.WordNet;
import net.didion.jwnl.JWNLException;
import scala.Serializable;
import scala.Tuple2;

public class DocumentSimilarityCalculator implements Serializable {

	private static final long serialVersionUID = -7658111802772631351L;

	private static Logger logger = LogManager.getLogger(DocumentSimilarityCalculator.class);

	private class WordSimilarityCalculator implements DoubleFunction<Tuple2<String, String>>, Serializable {

		private static final long serialVersionUID = -7631448644673167262L;

		private HashMap<String, HashSet<String>> synonymCache = null;

		WordSimilarityCalculator(HashMap<String, HashSet<String>> synonymCache) {

			this.synonymCache = synonymCache;
		}

		@Override
		public double call(Tuple2<String, String> wordPair) throws Exception {

			return wordSimilarityCommonSynonyms(wordPair._1(), wordPair._2());
		}

		/**
		 * computes the similarity between 2 words based on the ratio of shared synonyms (from all synonyms) which are retrieved from WordNet 3.0
		 *
		 * @param word1
		 * @param word2
		 * @return word similarity in [0,1]
		 * @throws JWNLException
		 * @throws FileNotFoundException
		 */
		private double wordSimilarityCommonSynonyms(String word1, String word2) throws JWNLException, FileNotFoundException {

			if (word1 == null || word1.length() == 0) return 0.0;
			if (word2 == null || word2.length() == 0) return 0.0;

			if (word1.equalsIgnoreCase(word2)) return 1.0;

			WordNet wn = new WordNet();

			HashSet<String> synonyms1 = synonymCache.get(word1);
			if (synonyms1.isEmpty()) return 0.0;
			HashSet<String> synonyms2 = synonymCache.get(word2);
			if (synonyms2.isEmpty()) return 0.0;

			// calc size of the intersection between the 2 hashsets	normalized to the size of the union
			Set<String> union = new HashSet<>(synonyms1);
			union.addAll(synonyms2);

			Set<String> intersection = new HashSet<>(synonyms1);
			intersection.retainAll(synonyms2);

			logger.info(word1 + " " + word2 + " have " + intersection.size() + " from " + union.size() + " synonyms in common");
			return intersection.size() / (double)union.size();
		}
	}

	private class TCComparator implements Comparator<Tuple2<String, Integer>>, Serializable {

		private static final long serialVersionUID = 3974942732647722026L;

		@Override
		public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {

			return t2._2().compareTo(t1._2());
		}
	}

	public JavaRDD<String> prepareDocument(JavaRDD<String> doc, int topNWords) throws IOException {

		// 1. tokenize
		doc = tokenize(doc);

		// 2. lower case
		doc = lowerCase(doc);

		// 3. remove stopwords
		doc = removeStopwords(doc);

		// 4. count, sort, and take top N Words
		return countAndSort(doc, topNWords).keys();
	}

	public HashSet<String> getSynonyms(String word) throws FileNotFoundException, JWNLException {

		WordNet wn = new WordNet();
		HashSet<String> synonyms = wn.getSynonyms(word);
		return synonyms;
	}

	public double calculateSemanticSimilarity(JavaRDD<String> doc1, JavaRDD<String> doc2, HashMap<String, HashSet<String>> synonymCache) {
		// compare each word from each document pairwise

		// 1. create cartesian product to represent all pairs
		JavaPairRDD<String, String> cp = doc1.cartesian(doc2);
		logger.info("comparing " + cp.count() + " pairs");

		// 2. compute similarity for each pair
		JavaDoubleRDD pairwiseSimilarities = cp.mapToDouble(new WordSimilarityCalculator(synonymCache));

		// 3. compute overall similarity as normalized sum
		return pairwiseSimilarities.sum() / pairwiseSimilarities.count();
	}

	private JavaRDD<String> tokenize(JavaRDD<String> input) {

		return input.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 4434644907723026843L;

			@Override
			public Iterator<String> call(String line) throws Exception {

				return new Tokenizer().tokenize(line).iterator();
			}
		});
	}

	private JavaRDD<String> lowerCase(JavaRDD<String> terms) {

		return terms.map(new Function<String, String>() {

			private static final long serialVersionUID = 1976635450595930028L;

			@Override
			public String call(String term) throws Exception {

				return term.toLowerCase();
			}
		});
	}

	private JavaRDD<String> removeStopwords(JavaRDD<String> terms) throws IOException {

		return terms.filter(new Function<String, Boolean>() {

			private static final long serialVersionUID = 4886135808449645833L;

			@Override
			public Boolean call(String term) throws Exception {

				return !new StopWords().isStopword(term);
			}
		});
	}

	private JavaPairRDD<String, Integer> countAndSort(JavaRDD<String> terms, int nbr) {

		JavaPairRDD<String, Integer> termsCounted = countTerms(terms);
		List<Tuple2<String, Integer>> termsCountedSorted = termsCounted.takeOrdered(nbr, new TCComparator());
		return Application.getSparkContext().parallelizePairs(termsCountedSorted);
	}

	private JavaPairRDD<String, Integer> countTerms(JavaRDD<String> terms) {

		// 1. transform the collection of terms into pairs (term,1)
		JavaPairRDD<String, Integer> termsSingle = terms.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 485312979993101389L;

			@Override
			public Tuple2<String, Integer> call(String s) {

				return new Tuple2<>(s, 1);
			}
		});

		// 2. count the terms
		JavaPairRDD<String, Integer> termsCounted = termsSingle.reduceByKey(new Function2<Integer, Integer, Integer>() {

			private static final long serialVersionUID = -5733428573133789840L;

			@Override
			public Integer call(Integer x, Integer y) {

				return x + y;
			}
		});

		return termsCounted;
	}
}
