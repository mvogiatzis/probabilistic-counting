package storm.state;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import backtype.storm.utils.Utils;

import model.Click;

import storm.trident.state.State;

public class UrlsDB implements State{

	private static final Logger LOG = Logger.getLogger(UrlsDB.class);
	
	private Map<String, Integer> urls;
	private Random r;
	private double rand;
	
	public UrlsDB(){
		urls = new HashMap<String, Integer>();
		r = new Random();
	}
	
	@Override
	public void beginCommit(Long txid) {
		
	}

	@Override
	public void commit(Long txid) {
		
	}
	
	/**
	 * Simulates a DB update
	 * @param key
	 * @param value
	 */
	public void incr(String key){
		Integer currValue = urls.get(key);
		if (currValue==null)	//check if exists
			currValue = 0;
		
		urls.put(key, currValue+1);
	}
	
	/**
	 * A DB Get. Returns 0 if the url is not found
	 * @param key
	 * @return int The current count
	 */
	public int get(String key){
		Integer currCount = urls.get(key);
		return (currCount==null) ? 0 : currCount;
	}
	
	/**
	 * Possibility to update the count
	 * @param url The given url
	 * @return int The count after the update
	 */
	public int updateCount(String url) {
			rand = r.nextDouble();	//generate a uniformly distributed double from 0 to 1.0
			int currValue = this.get(url); //get the current value
			double toCompare = Math.pow(2, -currValue);

			Utils.sleep(1000);
			LOG.info(url);
			if (rand < toCompare){	//only increment if random num is less than 1 / 2^f
				LOG.info("Random num = " + rand + " , " + " current_value = " + currValue + " , 1 / 2^-f = " + toCompare);
				LOG.info("Increment!");
				this.incr(url);
			}
			else{
				LOG.info("Random num = " + rand + " , " + " current_value = " + currValue + " , 1 / 2^-f = " + toCompare);
				LOG.info("No Increment!");
			}
			return this.get(url);
	}
	

}
