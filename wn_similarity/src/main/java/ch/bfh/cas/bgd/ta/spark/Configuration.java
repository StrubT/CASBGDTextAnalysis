package ch.bfh.cas.bgd.ta.spark;

import scala.Serializable;

public class Configuration implements Serializable {
	private static final long serialVersionUID = 6604060899716252008L;

	// Spark master node URL
	public static final String SPARK_MASTER_URL = "spark://bigdata-VirtualBox:7077";

	// application config
	public static final String APP_NAME = "similarity";
	
	// application dir
	private static final String APP_DIR = "/home/bigdata/develop/similarity";
	
	// tokenizer model
	public static final String TOKENIZER_MODEL = APP_DIR + "/data/en-token.bin";
	
	// stopwords filter
	public static final String STOPWORDS_FILE = APP_DIR + "/data/stopwords.txt";
	
	// wordnet properties
	public static final String WORDNET_PROPERTIES = APP_DIR + "/src/main/resources/properties.xml";
}
