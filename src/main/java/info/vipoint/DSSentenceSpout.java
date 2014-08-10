package info.vipoint;

import info.vipoint.utils.Utils;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;


//BaseRichSpout implements ISpout and IComponent interfaces 
public class DSSentenceSpout extends BaseRichSpout {
	
	private SpoutOutputCollector collector;
	private String[] sentences = {
			"my car has ABS",
			"is this a joke",
			"only top models come with ABS and Air Bags",
			"inferno orange stands out in cross variants"
	};
	private int index = 0;
	
	//defined in IComponent..what streams a component will emit and the fields each tuple will have
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}
	
	//defined in ISpout..called whenever a spout component is initialised
	public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}
	
	//in ISpout..spout emits tuples to the output collector
	public void nextTuple() {
		this.collector.emit(new Values(sentences[index]));
		index++;
		if(index >= sentences.length) {
			index = 0;
		}
		Utils.waitForMillis(1);
	}

}
