package storm.spout;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import model.Click;
import model.JsonReader;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * Reads Click data from the Stream or the File specified and
 * emits them into the system
 * 
 * @author michaelvogiatzis
 * 
 */
public class Spout extends BaseRichSpout {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5326274048147475478L;

	public static final String CLICK = "click";
	private SpoutOutputCollector _collector;
	private Random _rand;
	private JsonReader reader;
	private List<Click> clicks;
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
		reader = new JsonReader(new File("src/resources/usagov_bitly_data2013-04-19-1366384166"));
//		clicks = reader.selectLinesUpTo(10);	//populate the arraylist
		_rand = new Random();
	}

	@Override
	public void nextTuple() {
		Click click = reader.nextClick();
//		Click click = clicks.get(_rand.nextInt(10));	//get a random click

		if (click == null) {
			Utils.sleep(50);
		} else {
			_collector.emit(new Values(click));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(CLICK));
	}
	
	@Override
	public void close(){
		if (reader!=null)
			reader.shutdown();
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

}
