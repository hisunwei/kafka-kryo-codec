package kafka.kryo.example;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import kafka.kryo.KryCodec;

public class PersonKryoCodec extends KryCodec<Person> {

	public PersonKryoCodec() {

	}

	public static class PersonKryoSerializer extends Serializer<Person> {

		@Override
		public void write(Kryo kryo, Output output, Person object) {
		}

		@Override
		public Person read(Kryo kryo, Input input, Class<Person> type) {
			return null;
		}

	}

}
