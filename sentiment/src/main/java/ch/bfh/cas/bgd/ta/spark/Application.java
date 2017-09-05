
package ch.bfh.cas.bgd.ta.spark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import ch.bfh.cas.bgd.ta.nlp.Tokenizer;
import ch.bfh.cas.bgd.ta.nlp.sentiment.SentimentLexicon;
import ch.bfh.cas.bgd.ta.util.Review;
import scala.Serializable;
import scala.Tuple2;

public class Application implements Serializable {

	private static final long serialVersionUID = 3964335446668180836L;

	private static SparkConf sparkConf;
	private transient JavaSparkContext sparkContext;

	private static Logger logger = LogManager.getLogger(Application.class);

	static {
		sparkConf = new SparkConf().setMaster(Configuration.SPARK_MASTER_URL).setAppName(Configuration.APP_NAME);
	}

	public JavaSparkContext getSparkContext() {

		return sparkContext;
	}

	public static void main(String[] args) throws IOException {

		if (args.length != 0) {
			System.out.println("Usage: Application");
			System.exit(0);
		}

		Application app = new Application();

		// 1. read data
		logger.info("reading data...");
		JavaRDD<Review> reviews = app.readData();
		logger.info("read " + reviews.count() + " reviews");

		//Tokenizer tokenizer = new Tokenizer();
		//StopWords stopWords = new StopWords();
		//SentimentLexicon lexicon = new SentimentLexicon();

		JavaRDD<Review> reviewsCount = reviews //
			.map(r -> {
				Tokenizer tokenizer = new Tokenizer();
				SentimentLexicon lexicon = new SentimentLexicon();

				Map<Boolean, Long> counts = tokenizer.tokenize(r.getReview()).stream() //
					.map(t -> new Tuple2<>(lexicon.hasPositiveSentiment(t), lexicon.hasNegativeSentiment(t))) //
					.filter(p -> p._1() || p._2()) //
					.collect(Collectors.groupingBy(p -> p._1(), Collectors.counting()));

				double score = counts.get(true) / counts.values().stream().mapToDouble(l -> l).sum();

				Review v = new Review(r.getId(), r.getReview(), r.getClassGS());
				v.setClassSA(score * 2. - 1.);
				return v;
			});

		Files.write(Paths.get("asdf.txt"), //
			reviewsCount.collect().stream() //
				.map(r -> String.format("%s: %f vs. %f", r.getId(), r.getClassGS(), r.getClassSA())) //
				.collect(Collectors.toList()));

		//JavaRDD<Review>[] reviewPartitions = reviews.randomSplit(new double[] {
		//	.8,
		//	.2
		//});
		//JavaRDD<Review> reviewsTrain = reviewPartitions[0];
		//JavaRDD<Review> reviewsTest = reviewPartitions[1];
		//
		//JavaRDD<LabeledPoint> reviewsLP = reviewsTrain //
		//	.map(r -> new LabeledPoint(r.getClassGS(), r.getReview()));
		//
		//NaiveBayesModel model = NaiveBayes.train(reviewsLP.rdd());
		//model.predict(testData);
	}

	public Application() {

		sparkContext = new JavaSparkContext(sparkConf);
	}

	public JavaRDD<Review> readData() {

		JavaRDD<Review> reviews = readData(Configuration.DATA_DIR + "/neg", Configuration.SCORE_NEGATIVE);
		return reviews.union(readData(Configuration.DATA_DIR + "/pos", Configuration.SCORE_POSITIVE));
	}

	private JavaRDD<Review> readData(String path, double gsValue) {

		JavaPairRDD<String, String> files = sparkContext.wholeTextFiles(path);
		JavaPairRDD<String, Review> reviewsPair = files.mapToPair(new PairFunction<Tuple2<String, String>, String, Review>() {

			private static final long serialVersionUID = 6183921807258769850L;

			@Override
			public Tuple2<String, Review> call(Tuple2<String, String> review) throws Exception {

				return new Tuple2<>(review._1(), new Review(review._1(), review._2(), gsValue));
			}
		});
		JavaRDD<Review> reviews = reviewsPair.values();
		return reviews;
	}
}
