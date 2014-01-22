package storm.state;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import storm.bolts.LongUrlEmitter;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;

public class ProbCountBolt implements StateUpdater<UrlsDB>{
	
	public static final String CURR_URL_COUNT = "currUrlCount";
	int partIndex = 0;
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		partIndex = context.getPartitionIndex();
	}

	@Override
	public void updateState(UrlsDB state, List<TridentTuple> tuples,
			TridentCollector collector) {
		
		//for each url update it's count using
		//probabilistic counting algorithm
		for (TridentTuple tuple : tuples){
			String url = tuple.getStringByField(LongUrlEmitter.URL);
			int countAfterIncr = state.updateCount(url);
			collector.emit(new Values(url, countAfterIncr));	//emit url-count after increment
		}
		
		
	}
	
	

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}
	

}
