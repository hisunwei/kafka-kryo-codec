package kafka.kryo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import kafka.kryo.example.Person;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class KryCodec<T> extends Kryo implements kafka.serializer.Decoder<T>,
		kafka.serializer.Encoder<T> {

	@SuppressWarnings("unchecked")
	public KryCodec() {
		Type type = getClass().getGenericSuperclass();
		Type[] trueType = ((ParameterizedType) type).getActualTypeArguments();
		super.register((Class<T>) trueType[0]);
	}

	@Override
	@SuppressWarnings("unchecked")
	public T fromBytes(byte[] arg0) {
		Input input = new Input(new ByteArrayInputStream(arg0));
		return (T) readObject(input, Person.class);
	}

	@Override
	public byte[] toBytes(T arg0) {
		Output out = new Output(new ByteArrayOutputStream());
		super.writeObject(out, arg0);
		byte[] res = out.getBuffer();
		out.close();
		return res;
	}
}
