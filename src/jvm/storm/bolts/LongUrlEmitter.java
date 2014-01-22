package storm.bolts;

import backtype.storm.tuple.Values;
import model.Click;
import storm.spout.Spout;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Gets the click object and extracts the Long URL to count in next steps
 * 
 * @author michaelvogiatzis
 *
 */
public class LongUrlEmitter extends BaseFunction{
	public static final String URL = "longurl";
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Click click = (Click) tuple.getValueByField(Spout.CLICK);
		String longUrl = click.getLongURL();
		collector.emit(new Values(longUrl));
	}

}
