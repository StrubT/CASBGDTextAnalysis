package ch.bfh.cas.bgd.ta.spark;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Serializable;

public class Application implements Serializable {
	private static final long serialVersionUID = -3539379795242508287L;

	private static SparkConf sparkConf;
	
	private static JavaSparkContext sparkContext;

	static {
		sparkConf = new SparkConf().setMaster(Configuration.SPARK_MASTER_URL).setAppName(Configuration.APP_NAME);
		sparkContext = new JavaSparkContext(sparkConf);
	}

	public static JavaSparkContext getSparkContext() {
		return sparkContext;
	}

	public Application() {
	}

	public List<String> readGoldStandard(String file) throws FileNotFoundException {
		List<String> gs = new ArrayList<String>();

		Scanner scanner = new Scanner(new File(file));
		boolean personFound = false;
		StringBuffer person = new StringBuffer();
		while (scanner.hasNextLine()) {
			String[] termClass = scanner.nextLine().split("\\t");
			if (termClass.length != 3) continue;
			if (termClass[2].equalsIgnoreCase(Configuration.NER_PERSON)) {
				personFound = true;
				person.append(termClass[0] + " ");
			} else {
				if (personFound) { // add the current person
					personFound = false;
					gs.add(person.toString().trim());
					person = new StringBuffer();
				}
			}
		}
		if (personFound) { // do not forget the last one
			gs.add(person.toString().trim());
		}

		System.out.println("gold standard contains the following persons");
		Iterator<String> i = gs.iterator();
		while (i.hasNext()) {
			System.out.println(i.next());
		}

		scanner.close();

		return gs;
	}

	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.out.println("Usage: Application <textfile> <goldstandard>");
			System.exit(0);
		}

		// read gold standard
		Application app = new Application();
		List<String> nerGS = app.readGoldStandard(args[1]);

		// your code here
	}
}
