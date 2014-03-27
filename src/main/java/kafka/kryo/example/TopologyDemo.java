package kafka.kryo.example;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

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

public class TopologyDemo {

	public static class MySpout extends BaseRichSpout {
		private static final long serialVersionUID = 8179567498672462898L;
		ConsumerIterator<byte[], byte[]> it = null;
		PersonKryoCodec decoder = null;
		SpoutOutputCollector collector = null;

		@Override
		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			this.collector = collector;
			Properties props = PutGet.getProperties();
			ConsumerConfig consumerConfig = new ConsumerConfig(props);

			ConsumerConnector consumer = kafka.consumer.Consumer
					.createJavaConsumerConnector(consumerConfig);

			Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
			topicCountMap.put(PutGet.TOPIC, new Integer(1));

			Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
					.createMessageStreams(topicCountMap);

			List<KafkaStream<byte[], byte[]>> lists = consumerMap
					.get(PutGet.TOPIC);
			KafkaStream<byte[], byte[]> stream = lists.get(0);
			it = stream.iterator();
			decoder = new PersonKryoCodec();
		}

		@Override
		public void nextTuple() {
			if (it.hasNext()) {
				MessageAndMetadata<byte[], byte[]> msg = it.next();
				// System.out.println(stream + "  " + new String(msg.key())
				// + decoder.fromBytes(msg.message()));
				List<Object> tuples = new ArrayList<Object>();
				tuples.add(msg.key());
				tuples.add(msg.message());
				this.collector.emit(tuples);
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("key", "value"));
		}

	}

	public static class MyBolt extends BaseBasicBolt {
		private static final long serialVersionUID = -5078824970173175836L;
		PersonKryoCodec decoder = new PersonKryoCodec();

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			byte[] key = input.getBinaryByField("key");
			byte[] value = input.getBinaryByField("value");
			Person person = decoder.fromBytes(value);
			person.name = person.name + " bolt1";
			List<Object> tuples = new ArrayList<Object>();
			tuples.add(key);
			tuples.add(person);
			collector.emit(tuples);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("key", "value"));
		}

	}

	public static class MyBolt2 extends BaseBasicBolt {
		private static final long serialVersionUID = -5078824970173175836L;

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			byte[] key = input.getBinaryByField("key");
			Person value = (Person) input.getValueByField("value");
			System.out.println("bolt2 " + value);
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
		builder.setBolt("bolt2", new MyBolt2(), 1).shuffleGrouping("bolt1")
				.setMaxSpoutPending(10);// 每个task支持的pending数

		Config config = new Config();
		config.setNumWorkers(1);
		config.setNumAckers(1);
		config.registerSerialization(Person.class);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("xx", config, builder.createTopology());

	}

}
