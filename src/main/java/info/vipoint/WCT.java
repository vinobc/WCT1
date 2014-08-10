package info.vipoint;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import info.vipoint.utils.Utils;


public class WCT {
	
	//unique IDs for storm components
	private static final String SENTENCE_SPOUT_ID = "sentence-spout";
	private static final String BREAK_BOLT_ID = "split-bolt";
	private static final String WC_BOLT_ID = "count-bolt";
	private static final String RESULT_BOLT_ID = "report-bolt";
	private static final String TOPOLOGY_NAME = "wc-topology";
	
	public static void main(String[] args) {
		DSSentenceSpout spout = new DSSentenceSpout();
		BreakSentenceBolt breakBolt = new BreakSentenceBolt();
		WCBolt wcBolt = new WCBolt();
		ResultBolt resultBolt = new ResultBolt();
		
		//define the data flow between components
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(SENTENCE_SPOUT_ID, spout);
		builder.setBolt(BREAK_BOLT_ID, breakBolt).shuffleGrouping(SENTENCE_SPOUT_ID);
		builder.setBolt(WC_BOLT_ID, wcBolt).fieldsGrouping(BREAK_BOLT_ID,new Fields("word"));
		builder.setBolt(RESULT_BOLT_ID, resultBolt).globalGrouping(WC_BOLT_ID);
		
		//for topology's rt behavior
		Config config = new Config();
		
		LocalCluster cluster = new LocalCluster();
		
		cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
		Utils.waitForSeconds(10);
		cluster.killTopology(TOPOLOGY_NAME);
		cluster.shutdown();

	}

}
