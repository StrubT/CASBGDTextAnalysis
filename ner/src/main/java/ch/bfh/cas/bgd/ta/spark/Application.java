
package ch.bfh.cas.bgd.ta.spark;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import ch.bfh.cas.bgd.ta.nlp.Tokenizer;
import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.util.Span;
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

	public Application() {}

	public List<String> readGoldStandard(String file) throws FileNotFoundException {

		List<String> gs = new ArrayList<>();

		Scanner scanner = new Scanner(new File(file));
		boolean personFound = false;
		StringBuffer person = new StringBuffer();
		while (scanner.hasNextLine()) {
			String[] termClass = scanner.nextLine().split("\\t");
			if (termClass.length != 3) continue;
			if (termClass[2].equalsIgnoreCase(Configuration.NER_PERSON)) {
				personFound = true;
				person.append(termClass[0] + " ");
			} else if (personFound) { // add the current person
				personFound = false;
				gs.add(person.toString().trim());
				person = new StringBuffer();
			}
		}
		if (personFound) gs.add(person.toString().trim());

		//System.out.println("gold standard contains the following persons");
		//Iterator<String> i = gs.iterator();
		//while (i.hasNext())
		//	System.out.println(i.next());

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

		String text = new String(Files.readAllBytes(Paths.get(args[0])));
		Tokenizer tokenizer = new Tokenizer();
		String[] tokens = tokenizer.tokenize(text).toArray(new String[] {});

		TokenNameFinderModel model = new TokenNameFinderModel(Paths.get(Configuration.PERSON_MODEL));
		NameFinderME nameFinder = new NameFinderME(model);
		Span[] nameSpans = nameFinder.find(tokens);
		List<String> names = Arrays.asList(Span.spansToStrings(nameSpans, tokens));

		Path resultPath = Paths.get(Configuration.RESULT);
		Files.write(resultPath, Arrays.asList("*** Gold Standard ***"));
		Files.write(resultPath, nerGS.stream().distinct().sorted().collect(Collectors.toList()), StandardOpenOption.APPEND);

		Files.write(resultPath, Arrays.asList("", "*** My Result ***"), StandardOpenOption.APPEND);
		Files.write(resultPath, names.stream().distinct().sorted().collect(Collectors.toList()), StandardOpenOption.APPEND);

		Map<String, Long> mapGS = nerGS.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
		Map<String, Long> mapNames = names.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

		Set<String> keyIntersection = new HashSet<>(mapGS.keySet());
		keyIntersection.retainAll(mapNames.keySet());
		Map<String, Long> mapIntersection = keyIntersection.stream() //
			.collect(Collectors.toMap(Function.identity(), k -> Math.min(mapGS.get(k), mapNames.get(k))));

		Long countGS = mapGS.values().stream().mapToLong(l -> l).sum();
		Long countNames = mapNames.values().stream().mapToLong(l -> l).sum();
		Long countIntersection = mapIntersection.values().stream().mapToLong(l -> l).sum();
		Files.write(resultPath, Arrays.asList("", //
			String.format("Precision: %f (%d / %d)", (double)countIntersection / countNames, countIntersection, countNames), //
			String.format("Recall: %f (%d / %d)", (double)countIntersection / countGS, countIntersection, countGS)), //
			StandardOpenOption.APPEND);
	}
}
