package model;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import backtype.storm.utils.Utils;

/**
 * Helper class to read json from a Stream or a File
 * 
 * @author michaelvogiatzis
 * 
 */
public class JsonReader {

	private BufferedReader br;
	private InputStream is;
	static final Logger LOG = Logger.getLogger(JsonReader.class);

	public JsonReader(String url) {
		try {
			is = new URL(url).openStream();
			br = new BufferedReader(new InputStreamReader(is,
					Charset.forName("UTF-8")));
			String jsonText = readAll(br);
		} catch (Exception m) {
			LOG.error(m.toString());
		}
	}

	public JsonReader(File fileToReadFrom) {
		try {
			br = new BufferedReader(new FileReader(fileToReadFrom));

		} catch (FileNotFoundException e) {
			LOG.error(e.toString());
		}
	}
	

	/**
	 * Returns the next {@link Click} object
	 * 
	 * @return Click
	 */
	public Click nextClick() {
		String inputLine = null;
		try {
			while ((inputLine = br.readLine()) != null) {
				if (!inputLine.contains("_heartbeat_")
						&& inputLine.length() > 0) {
					Click click = new Click();
					click.populate(inputLine);
					return click;
				}
			}
		} catch (IOException e) {
			LOG.error(e.toString());
		}
		return null;
	}
	

	private String readAll(Reader rd) throws IOException {
		StringBuilder sb = new StringBuilder();
		int cp;
		while ((cp = rd.read()) != -1) {
			sb.append((char) cp);
		}
		return sb.toString();
	}

	public void shutdown() {
		if (br != null) {
			try {
				br.close();
			} catch (IOException e) {
				LOG.error(e.toString());
			}
		}
	}

	public void crawl() throws Exception {
		URL usaGovURL = new URL("http://developer.usa.gov/1usagov");
		BufferedReader in = new BufferedReader(new InputStreamReader(
				usaGovURL.openStream()));

		String inputLine;

		// write
		File file = new File(
				"file_to_store.json");

		// if file doesnt exists, then create it
		if (!file.exists()) {
			file.createNewFile();
		}

		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		while ((inputLine = in.readLine()) != null) {
			if (!inputLine.contains("_heartbeat_")) {
				bw.write(inputLine);
				bw.newLine();
				bw.flush();
			}
		}
	}

	/**
	 * Selects the json objects up to the given line exclusive
	 * @param line
	 * @return List<Click> A List of Click objects
	 */
	public List<Click> selectLinesUpTo(int line) {
		List<Click> clicks = new ArrayList<Click>();
		Click c=null;
		int i=0;
		while ((c = nextClick()) != null && i < line){
			clicks.add(c);
			i++;
		}
		return clicks;
	}

}
