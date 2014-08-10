package info.vipoint;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

//BaseRichBolt implements IComponent and IBolt
public class BreakSentenceBolt extends BaseRichBolt {
	private OutputCollector collector;
	
	//IBolt...similar to open()..initialization
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	
	//IBolt.. called every time the bolt receives a tuple from a stream to which it subscribes
	public void execute(Tuple tuple) {
		String sentence = tuple.getStringByField("sentence");
		String[] words = sentence.split(" ");
		for(String word : words) {
			this.collector.emit(new Values(word));
		}
	}
	
	//IComponent..tuple form (one field for each tuple)
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
	

}
