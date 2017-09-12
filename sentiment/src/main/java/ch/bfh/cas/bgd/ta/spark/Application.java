
package ch.bfh.cas.bgd.ta.spark;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

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

		JavaRDD<Review>[] reviewSplits = reviews.randomSplit(new double[] {
			Configuration.TRAINING_SET_RATIO,
			Configuration.TEST_SET_RATIO
		});
		JavaRDD<Review> reviewsTraining = reviewSplits[0];
		JavaRDD<Review> reviewsTest = reviewSplits[1];

		Function<Review, Vector> getFeatures = (Function<Review, Vector> & Serializable)r -> {

			Tokenizer tokenizer;
			SentimentLexicon lexicon;

			try {
				tokenizer = new Tokenizer();
				lexicon = new SentimentLexicon();

			} catch (IOException ex) {
				throw new UncheckedIOException(ex);
			}

			Object keyTrue = new Object();
			Object keyFalse = new Object();

			Map<Object, Long> counts = tokenizer.tokenize(r.getReview()).stream() //
				.map(t -> new Tuple2<>(lexicon.hasPositiveSentiment(t), lexicon.hasNegativeSentiment(t))) //
				.filter(p -> p._1() || p._2()) //
				.collect(Collectors.groupingBy(p -> p._1() ? keyTrue : keyFalse, Collectors.counting()));

			double f0 = counts.values().stream().mapToDouble(c -> c).sum();
			double f1 = counts.getOrDefault(keyTrue, 0L) / f0;
			double f2 = counts.getOrDefault(keyFalse, 0L) / f0;

			return Vectors.dense(f1, f2);
		};

		JavaRDD<LabeledPoint> pointsTraining = reviewsTraining.map(r -> new LabeledPoint(r.getClassGS(), getFeatures.apply(r)));
		NaiveBayesModel model = NaiveBayes.train(pointsTraining.rdd());

		JavaRDD<Review> testResults = reviewsTest.map(r -> {

			Review review = new Review(r.getId(), r.getReview(), r.getClassGS());
			review.setClassSA(model.predict(getFeatures.apply(r)));
			return review;
		});

		Files.write(Paths.get("result.txt"), //
			testResults.collect().stream() //
				.map(r -> String.format("%s: %f vs. %f", r.getId(), r.getClassGS(), r.getClassSA())) //
				.collect(Collectors.toList()));
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
