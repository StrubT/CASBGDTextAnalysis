
package ch.bfh.cas.bgd.ta.spark;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import net.didion.jwnl.JWNLException;
import scala.Serializable;

public class Application implements Serializable {

	private static final long serialVersionUID = -3539379795242508287L;

	private static SparkConf sparkConf;

	private static JavaSparkContext sparkContext;

	private static Logger logger = LogManager.getLogger(Application.class);

	static {
		sparkConf = new SparkConf().setMaster(Configuration.SPARK_MASTER_URL).setAppName(Configuration.APP_NAME);
		sparkContext = new JavaSparkContext(sparkConf);
	}

	public static JavaSparkContext getSparkContext() {

		return sparkContext;
	}

	public Application() {}

	public static void main(String[] args) throws IOException, JWNLException {

		Application app = new Application();
		DocumentSimilarityCalculator dsc = new DocumentSimilarityCalculator();

		FileWriter fw = null;
		fw = new FileWriter(Configuration.RESULT);

		// prepare documents
		HashMap<String, HashSet<String>> synonymCache = new HashMap<>();
		List<JavaRDD<String>> docs = new ArrayList<>();
		for (int i = 0; i < args.length; ++i) {
			logger.info("prepare document " + i + ": " + args[i]);
			JavaRDD<String> doc = Application.getSparkContext().textFile(args[i]); // splits the input file into lines
			doc = dsc.prepareDocument(doc, Configuration.TOP_N_WORDS_4_SIMILARITY);
			docs.add(doc);

			Iterator<String> w = doc.collect().iterator();
			while (w.hasNext()) {
				String word = w.next();
				if (!synonymCache.containsKey(word)) {
					HashSet<String> synonyms = dsc.getSynonyms(word);
					synonymCache.put(word, synonyms);
					logger.info(word + " has " + synonyms.size());
				}
			}
		}

		// compare docs pairwise and store the result
		for (int i = 0; i < docs.size(); ++i)
			for (int j = i; j < docs.size(); ++j) { // also compare each doc to itself
				logger.info("compare documents " + i + " and " + j);
				Double result = dsc.calculateSemanticSimilarity(docs.get(i), docs.get(j), synonymCache);
				logger.info("result " + result.toString());

				fw.write(i + " " + j + " " + result.toString() + "\n");

				if (i != j) fw.write(j + " " + i + " " + result.toString() + "\n");
			}

		fw.close();
	}
}
