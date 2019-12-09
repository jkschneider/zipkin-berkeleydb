package zipkin2.module.storage.lucene;

import org.jetbrains.annotations.NotNull;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.IOException;

public class Roaring64NavigableMapSerializer implements Serializer<Roaring64NavigableMap> {
  public static Roaring64NavigableMapSerializer INSTANCE = new Roaring64NavigableMapSerializer();

  private Roaring64NavigableMapSerializer() {
  }

  @Override
  public void serialize(@NotNull DataOutput2 out, @NotNull Roaring64NavigableMap value) throws IOException {
    value.serialize(out);
  }

  @Override
  public Roaring64NavigableMap deserialize(@NotNull DataInput2 input, int available) throws IOException {
    Roaring64NavigableMap rbmp = new Roaring64NavigableMap();
    if (available > 0) {
      rbmp.deserialize(input);
    }
    return rbmp;
  }
}
