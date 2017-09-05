package ch.bfh.cas.bgd.ta.spark;

import scala.Serializable;

public class Configuration implements Serializable {
	private static final long serialVersionUID = 6604060899716252008L;

	// Spark master node URL
	public static final String SPARK_MASTER_URL = "spark://bigdata-VirtualBox:7077";

	// application config
	public static final String APP_NAME = "ner";
	
	// application dir
	private static final String APP_DIR = "/home/bigdata/develop/ner";
	
	// sentence detection model
	public static final String SENTENCE_MODEL = APP_DIR + "/data/en-sent.bin";

	// tokenizer model
	public static final String TOKENIZER_MODEL = APP_DIR + "/data/en-token.bin";
	
	
	// NER
	public static final String NER_PERSON = "PERS";
	public static final String PERSON_MODEL = APP_DIR + "/data/en-ner-person.bin";

	// result
	public static final String RESULT = "result.txt";
}
