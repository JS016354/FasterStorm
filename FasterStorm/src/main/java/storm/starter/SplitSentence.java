package storm.starter;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.google.common.collect.Lists;

public class SplitSentence extends BaseBasicBolt {
	private static final long serialVersionUID = 146904577782804083L;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		for (String split : input.getString(0).split(" ")) {
			collector.emit(Lists.<Object> newArrayList(split));
		}
	}
}