package kafka.kryo;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

public class KryoEncoder<T> extends Kryo implements kafka.serializer.Encoder<T> {

	@SuppressWarnings("unchecked")
	public KryoEncoder() {
		Type type = getClass().getGenericSuperclass();
		Type[] trueType = ((ParameterizedType) type).getActualTypeArguments();
		super.register((Class<T>) trueType[0]);
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
