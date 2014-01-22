package storm.topology;

import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Fields;
import storm.bolts.LongUrlEmitter;
import storm.spout.Spout;
import storm.state.ProbCountBolt;
import storm.state.UrlsDB;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Debug;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.utils.FirstNAggregator;

/**
 * Runs the system
 * 
 * @author michaelvogiatzis
 * 
 */
public class ProbCountTopology {

	public static final String TOPOLOGY_NAME = "ProbCountTopology";

	/**
	 * Topology builder
	 * @return
	 */
	public static StormTopology buildTopology() {

		TridentTopology topology = new TridentTopology();

		TridentState urlsState = topology.newStaticState(new UrlsDBFactory());
		//define the stream
		topology.newStream("countStream", new Spout())
		.each(new Fields(Spout.CLICK), new LongUrlEmitter(), new Fields(LongUrlEmitter.URL))
//			.parallelismHint(5)
		.partitionBy(new Fields(LongUrlEmitter.URL))
		.partitionPersist(new UrlsDBFactory(), new Fields(LongUrlEmitter.URL), new ProbCountBolt(), 
				new Fields(LongUrlEmitter.URL, ProbCountBolt.CURR_URL_COUNT))
//				.parallelismHint(5)
				.newValuesStream()
				.shuffle()
				.aggregate(new Fields(LongUrlEmitter.URL, ProbCountBolt.CURR_URL_COUNT), 
						new FirstNAggregator(1, ProbCountBolt.CURR_URL_COUNT, true), 
						new Fields(LongUrlEmitter.URL, ProbCountBolt.CURR_URL_COUNT))
				.each(new Fields(LongUrlEmitter.URL, ProbCountBolt.CURR_URL_COUNT), new Debug());

		return topology.build();
	}

	public static class UrlsDBFactory implements StateFactory {
		@Override
		public State makeState(Map conf, IMetricsContext metrics,
				int partitionIndex, int numPartitions) {
			return new UrlsDB();
		}

	}

	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException {
		Config conf = new Config();
//		conf.setDebug(true);
		if (args == null || args.length == 0) {
			// local mode
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(TOPOLOGY_NAME, conf, buildTopology());
		} else {
			// distributed mode

			conf.put(Config.STORM_CLUSTER_MODE, new String("distributed"));
			conf.setNumWorkers(1);

			StormSubmitter.submitTopology(args[0], conf, buildTopology());
		}
	}

}
