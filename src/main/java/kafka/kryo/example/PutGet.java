package kafka.kryo.example;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class PutGet {
	final static String TOPIC = "xxxxx";
	final static String GROUP = "t";

	public static void main(String[] args) {
		put();
//		get();
	}

	public static Properties getProperties() {
		Properties props = new Properties();
		props.put("metadata.broker.list", "localhost:9092");
		props.put("zookeeper.connect", "localhost:2181");
		props.put("serializer.class", "kafka.serializer.DefaultEncoder");
		props.put("key.serializer.class", "kafka.serializer.StringEncoder");
		// props.put("partitioner.class", );
		props.put("zk.sessiontimeout.ms", 60000);
		props.put("zk.synctime.ms", 500);
		props.put("request.required.acks", "1");
		props.put("group.id", GROUP);
		return props;
	}

	public static void put() {
		Properties props = getProperties();

		Random rnd = new Random();
		ProducerConfig config = new ProducerConfig(props);

		Producer<String, byte[]> producer = new Producer<String, byte[]>(config);

		PersonKryoCodec encoder = new PersonKryoCodec();
		for (long nEvents = 0; nEvents < 10; nEvents++) {
			long runtime = new Date().getTime();
			String ip = "192.168.2." + rnd.nextInt(255);
			String msg = runtime + ",www.example.com," + ip;
			System.out.println("put:" + ip + " " + msg);
			Person ooxx = new Person(msg, new Date());
			byte[] bytes = encoder.toBytes(ooxx);
			KeyedMessage<String, byte[]> data = new KeyedMessage<String, byte[]>(
					TOPIC, ip, bytes);
			producer.send(data);
		}
		producer.close();
	}

	public static void get() {
		Properties props = getProperties();
		ConsumerConfig consumerConfig = new ConsumerConfig(props);

		ConsumerConnector consumer = kafka.consumer.Consumer
				.createJavaConsumerConnector(consumerConfig);

		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(TOPIC, new Integer(1));

		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
				.createMessageStreams(topicCountMap);

		List<KafkaStream<byte[], byte[]>> lists = consumerMap.get(TOPIC);
		KafkaStream<byte[], byte[]> stream = lists.get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		PersonKryoCodec decoder = new PersonKryoCodec();
		while (it.hasNext()) {
			MessageAndMetadata<byte[], byte[]> msg = it.next();
			System.out.println(stream + "  " + new String(msg.key())
					+ decoder.fromBytes(msg.message()));
		}
	}
}
