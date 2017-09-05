package ch.bfh.cas.bgd.ta.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Scanner;

import scala.Serializable;

public class TextFile implements Serializable {
	private static final long serialVersionUID = -743705015706456706L;

	public static String readFile(String pathname) throws IOException {
	    File file = new File(pathname);
	    StringBuilder fileContents = new StringBuilder((int)file.length());
	    Scanner scanner = new Scanner(file);
	    String lineSeparator = System.getProperty("line.separator");

	    try {
	        while(scanner.hasNextLine()) {        
	            fileContents.append(scanner.nextLine() + lineSeparator);
	        }
	        return fileContents.toString();
	    } finally {
	        scanner.close();
	    }
	}
	
	public static HashSet<String> readFileAsHashSet(String filename) throws FileNotFoundException {
		File file = new File(filename);
	    Scanner scanner = new Scanner(file);
		return readLinewise(scanner);
	}

	private static HashSet<String> readLinewise(Scanner scanner) {
		HashSet<String> words = new HashSet<String>();
	    while (scanner.hasNext()) { // one word per line
	    	words.add(scanner.next());
	    }
	    scanner.close();
		return words;
	}
}
