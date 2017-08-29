
package ch.bfh.cas.bgd.ta.spark;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import ch.bfh.cas.bgd.ta.nlp.StopWords;
import ch.bfh.cas.bgd.ta.nlp.Tokenizer;
import ch.bfh.cas.bgd.ta.nlp.WordNet;
import net.didion.jwnl.JWNLException;
import scala.Serializable;
import scala.Tuple2;

public class Application implements Serializable {

	private static final long serialVersionUID = -3539379795242508287L;

	private static final SparkConf sparkConf;
	private static final JavaSparkContext sparkContext;

	private static final WordNet wordNet;
	private static final Map<String, Set<String>> synonymCache;

	static {

		try {
			sparkConf = new SparkConf().setMaster(Configuration.SPARK_MASTER_URL).setAppName(Configuration.APP_NAME);
			sparkContext = new JavaSparkContext(sparkConf);

			wordNet = new WordNet();
			synonymCache = new HashMap<>();

		} catch (FileNotFoundException | JWNLException ex) {
			throw new RuntimeException(ex);
		}
	}

	public static JavaSparkContext getSparkContext() {

		return sparkContext;
	}

	public static void main(String[] args) throws IOException, JWNLException {

		try (JavaSparkContext sc = getSparkContext()) {

			Tokenizer tokenizer = new Tokenizer();
			StopWords stopWords = new StopWords();

			List<List<String>> documents = Arrays.stream(args) //
				.map(f -> sc.textFile(f) //
					.flatMap(s -> tokenizer.tokenize(s).iterator()) //
					.map(String::toLowerCase) //
					.filter(s -> !stopWords.isStopword(s)) //
					.mapToPair(s -> new Tuple2<>(s, 1)) //
					.reduceByKey((x, y) -> x + y) //
					.takeOrdered(250, (Comparator<Tuple2<String, Integer>> & Serializable)(x, y) -> y._2().compareTo(x._2())) //
					.stream().map(p -> p._1()).collect(Collectors.toList())) //
				.collect(Collectors.toList());

			IntStream.range(0, documents.size()).forEach(i -> {
				try {
					Files.write(Paths.get(String.format("doc-%d.txt", i++)), documents.get(i));
				} catch (IOException ex) {
					throw new UncheckedIOException(ex);
				}
			});
		}
	}

	private static Set<String> getSynonyms(String word) throws JWNLException {

		Set<String> synonyms = synonymCache.get(word);
		if (synonyms == null) synonymCache.put(word, synonyms = wordNet.getSynonyms(word));

		return synonyms;
	}

	private static double calculateWordSimilarity(String word1, String word2) throws JWNLException {

		if (word1 == null || word1.length() == 0 || word2 == null || word2.length() == 0) return 0.;
		if (word1.equalsIgnoreCase(word2)) return 1.;

		Set<String> synonyms1 = getSynonyms(word1);
		Set<String> synonyms2 = getSynonyms(word2);
		if (synonyms1.isEmpty() || synonyms2.isEmpty()) return 0.;

		Set<String> union = new HashSet<>(synonyms1);
		union.addAll(synonyms2);

		Set<String> intersection = new HashSet<>(synonyms1);
		intersection.retainAll(synonyms2);

		return (double)intersection.size() / union.size();
	}
}
