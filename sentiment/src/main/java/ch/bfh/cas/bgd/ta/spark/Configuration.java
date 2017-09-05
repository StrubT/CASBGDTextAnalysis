package ch.bfh.cas.bgd.ta.spark;

import scala.Serializable;

public class Configuration implements Serializable {
	private static final long serialVersionUID = 4938824276371574395L;

	// Spark master node URL
	public static final String SPARK_MASTER_URL = "spark://bigdata-VirtualBox:7077";

	// application config
	public static final String APP_NAME = "sentiment";
	
	// application dir
	private static final String APP_DIR = "/home/bigdata/develop/sentiment";
	
	// test and training data
	public static final String DATA_DIR = APP_DIR + "/data/txt_sentoken";
	public static final double TEST_SET_RATIO = 0.2;
	public static final double TRAINING_SET_RATIO = 0.8;	
	
	// sentiment lexicon
	public static final String NEGATIVE_WORDS_LOCAL = APP_DIR + "/data/negative-words.txt";
	public static final String POSITIVE_WORDS_LOCAL = APP_DIR + "/data/positive-words.txt";
	
	// tokenizer model
	public static final String TOKENIZER_MODEL = APP_DIR + "/data/en-token.bin";
	
	// stopwords filter
	public static final String STOPWORDS_FILE = APP_DIR + "/data/stopwords.txt";
	
	// encoding of result values
	public static final double SCORE_NEGATIVE = -1.0;
	public static final double SCORE_NEUTRAL = 0.0;
	public static final double SCORE_POSITIVE = 1.0;

}
