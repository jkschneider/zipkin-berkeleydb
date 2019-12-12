package zipkin2.module.storage.lucene;

import org.jetbrains.annotations.NotNull;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import java.io.IOException;
import java.util.Objects;

public class UniqueSpanId implements Comparable<UniqueSpanId> {
    public static Serializer<UniqueSpanId> SERIALIZER = new Serializer<UniqueSpanId>() {
        @Override
        public void serialize(@NotNull DataOutput2 out, @NotNull UniqueSpanId value) throws IOException {
            out.writeUTF(value.traceId);
            out.writeUTF(value.spanId);
        }

        @Override
        public UniqueSpanId deserialize(@NotNull DataInput2 input, int available) throws IOException {
            return new UniqueSpanId(input.readUTF(), input.readUTF());
        }
    };

    private final String traceId;
    private final String spanId;

    public UniqueSpanId(String traceId, String spanId) {
        this.traceId = traceId;
        this.spanId = spanId;
    }

    public String getTraceId() {
        return traceId;
    }

    public String getSpanId() {
        return spanId;
    }

    @Override
    public int compareTo(@NotNull UniqueSpanId o) {
        int traceCompare = traceId.compareTo(o.traceId);
        return traceCompare == 0 ? spanId.compareTo(o.spanId) : traceCompare;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UniqueSpanId that = (UniqueSpanId) o;
        return traceId.equals(that.traceId) &&
          spanId.equals(that.spanId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(traceId, spanId);
    }
}
