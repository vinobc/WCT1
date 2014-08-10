package info.vipoint;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WCBolt extends BaseRichBolt {
	private OutputCollector colletor;
	
	//will store all the words and their counts
	private HashMap<String, Long> counts = null;
	
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.colletor = collector;
		this.counts = new HashMap<String, Long>();
	}
	
	public void execute(Tuple tuple) {
		String word = tuple.getStringByField("word");
		Long count = this.counts.get(word);
		if (count == null) {
			count = 0L;
		}
		count++;
		this.counts.put(word, count);
		this.colletor.emit(new Values(word,count));
		
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word","count"));
	}

}
