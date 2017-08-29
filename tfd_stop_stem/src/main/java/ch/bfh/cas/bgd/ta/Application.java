
package ch.bfh.cas.bgd.ta;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.tartarus.Stemmer;

import nz.ac.waikato.IteratedLovinsStemmer;
import nz.ac.waikato.LovinsStemmer;
import scala.Tuple2;

public class Application {

	private SparkConf sparkConf;
	private JavaSparkContext sparkContext;

	public Application() {

		sparkConf = new SparkConf().setMaster(Configuration.SPARK_MASTER_URL).setAppName(Configuration.APP_NAME);
		sparkContext = new JavaSparkContext(sparkConf);
	}

	public JavaSparkContext getSparkContext() {

		return sparkContext;
	}

	public static void main(String[] args) {

		if (args.length == 0) {
			System.out.println("Usage: Application <file>");
			System.exit(0);
		}

		List<String> stopwords;
		try (InputStream stream = Application.class.getResourceAsStream("stopwords.txt");
			BufferedReader streamReader = new BufferedReader(new InputStreamReader(stream))) {
			stopwords = streamReader.lines().collect(Collectors.toList());

		} catch (IOException ex) {
			throw new UncheckedIOException(ex);
		}

		Application application = new Application();
		try (JavaSparkContext sc = application.getSparkContext()) {

			Stemmer porterStemmer = new Stemmer();
			LovinsStemmer lovinsStemmer = new IteratedLovinsStemmer();
			Pattern pattern = Pattern.compile("\\W+");
			Logger logger = Logger.getLogger(Application.class);

			Map<String, Integer> baseResults = sc.textFile(args[0]) //
				.flatMap(s -> pattern.splitAsStream(s).iterator()) //
				.mapToPair(s -> new Tuple2<>(s, 1)) //
				.filter(t -> t._1().length() > 0) //
				.reduceByKey((x, y) -> x + y) //
				.collectAsMap();
			logger.info(String.format("#base results: %d", baseResults.size()));

			Map<String, Integer> lowerCaseResults = sc.textFile(args[0]) //
				.flatMap(s -> pattern.splitAsStream(s).iterator()) //
				.map(s -> s.toLowerCase()) //
				.mapToPair(s -> new Tuple2<>(s, 1)) //
				.filter(t -> t._1().length() > 0) //
				.reduceByKey((x, y) -> x + y) //
				.collectAsMap();
			logger.info(String.format("#lower case results: %d", lowerCaseResults.size()));

			Map<String, Integer> stopwordsResults = sc.textFile(args[0]) //
				.flatMap(s -> pattern.splitAsStream(s).iterator()) //
				.map(s -> s.toLowerCase()) //
				.filter(s -> !stopwords.contains(s)) //
				.mapToPair(s -> new Tuple2<>(s, 1)) //
				.filter(t -> t._1().length() > 0) //
				.reduceByKey((x, y) -> x + y) //
				.collectAsMap();
			logger.info(String.format("#results w/o stopwords: %d", stopwordsResults.size()));

			Map<String, Integer> lovinsStemmerResults = sc.textFile(args[0]) //
				.flatMap(s -> pattern.splitAsStream(s).iterator()) //
				.map(s -> s.toLowerCase()) //
				.filter(s -> !stopwords.contains(s)) //
				.map(s -> lovinsStemmer.stem(s)) //
				.mapToPair(s -> new Tuple2<>(s, 1)) //
				.filter(t -> t._1().length() > 0) //
				.reduceByKey((x, y) -> x + y) //
				.collectAsMap();
			logger.info(String.format("#lovins stemmer results: %d", lovinsStemmerResults.size()));
		}
	}
}
