package kafka.kryo.example;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class TopologyDemo2 {

	public static class MySpout extends BaseRichSpout {
		private static final long serialVersionUID = 8179567498672462898L;
		SpoutOutputCollector collector = null;
		PersonKryoCodec encoder = null;
		int c = 0;

		@Override
		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			this.collector = collector;
			encoder = new PersonKryoCodec();
		}

		@Override
		public void nextTuple() {
			List<Object> objs = new ArrayList<Object>();

			objs.add(encoder.toBytes(new Person("Zoie" + c, new Date())));
			this.collector.emit(objs, c++);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("person"));
		}

	}

	public static class MyBolt extends BaseBasicBolt {
		private static final long serialVersionUID = -5078824970173175836L;
		PersonKryoCodec decoder = null;

		@Override
		public void prepare(Map stormConf, TopologyContext context) {
			// TODO Auto-generated method stub
			super.prepare(stormConf, context);
			decoder = new PersonKryoCodec();
		}

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
//			Person person = decoder.fromBytes(input.getBinaryByField("person"));
			Person person = (Person)input.getValueByField("person");
			person.name = person.name + " bolt1";
			System.out.println(person);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
		}

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new MySpout()).setMaxSpoutPending(1 * 10);// spout的支持的pending也就是
		builder.setBolt("bolt1", new MyBolt(), 1).shuffleGrouping("spout")
				.setMaxSpoutPending(10);// 每个task支持的pending数

		Config config = new Config();
		config.setNumWorkers(1);
		config.setNumAckers(1);
		config.registerSerialization(Person.class);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("xx", config, builder.createTopology());

	}

}
