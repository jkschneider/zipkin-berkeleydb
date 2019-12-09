package zipkin2.module.storage.lucene;

import org.jetbrains.annotations.NotNull;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;
import zipkin2.Span;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class SpanSerializer implements Serializer<Span> {
  @Override
  public void serialize(@NotNull DataOutput2 out, @NotNull Span value) throws IOException {
    try (ObjectOutputStream oos = new ObjectOutputStream(out)) {
      oos.writeObject(value);
    }
  }

  @Override
  public Span deserialize(@NotNull DataInput2 input, int available) throws IOException {
    try (InputStream dis = new DataInput2.DataInputToStream(input);
         ObjectInputStream ois = new ObjectInputStream(dis)) {
      return (Span) ois.readObject();
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Failed to deserialize span", e);
    }
  }
}
