package zipkin2.module.storage.lucene;

import com.google.common.collect.Streams;
import io.micrometer.core.instrument.util.StringUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import zipkin2.Annotation;
import zipkin2.Call;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.storage.*;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;

import static java.util.stream.Collectors.joining;
import static org.apache.lucene.document.Field.Store.NO;

public class LuceneStorage extends StorageComponent {
  private final LuceneSpanConsumer spanConsumer;
  private final LuceneSpanStore spanStore;

  private final DB db;
  private final HTreeMap<String, Roaring64NavigableMap> spanIdsByServiceName;
  private final HTreeMap<String, Roaring64NavigableMap> spanIdsByRemoteServiceName;
  private final HTreeMap<String, Roaring64NavigableMap> spanIdsByTraceId;
  private final HTreeMap<String, Span> spanBySpanId;

  private LuceneStorage(File indexDirectory) {
    this.spanConsumer = new LuceneSpanConsumer(indexDirectory);
    this.spanStore = new LuceneSpanStore();

    this.db = DBMaker
      .fileDB(new File(indexDirectory, "map.db"))
      .closeOnJvmShutdown()
      .fileMmapEnableIfSupported()
      .make();

    this.spanIdsByTraceId = searchByCriteriaMap("traceId");
    this.spanIdsByServiceName = searchByCriteriaMap("serviceName");
    this.spanIdsByRemoteServiceName = searchByCriteriaMap("remoteServiceName");
    this.spanBySpanId = db.hashMap("spanId")
      .keySerializer(Serializer.STRING)
      .valueSerializer(new SpanSerializer())
      .createOrOpen();
  }

  public static Builder newBuilder(File indexDirectory) {
    return new Builder(indexDirectory);
  }

  private HTreeMap<String, Roaring64NavigableMap> searchByCriteriaMap(String criteria) {
    return db
      .hashMap(criteria)
      .keySerializer(Serializer.STRING)
      .valueSerializer(Roaring64NavigableMapSerializer.INSTANCE)
      .createOrOpen();
  }

  @Override
  public void close() throws IOException {
    this.spanConsumer.close();
  }

  @Override
  public SpanStore spanStore() {
    return spanStore;
  }

  @Override
  public SpanConsumer spanConsumer() {
    return spanConsumer;
  }

  public static final class Builder extends StorageComponent.Builder {
    private final File indexDirectory;

    public Builder(File indexDirectory) {
      this.indexDirectory = indexDirectory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Builder strictTraceId(boolean strictTraceId) {
      if (!strictTraceId) {
        throw new UnsupportedOperationException("strictTraceId cannot be disabled");
      }
      return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Builder searchEnabled(boolean searchEnabled) {
      if (!searchEnabled) {
        throw new UnsupportedOperationException("searchEnabled cannot be disabled");
      }
      return this;
    }

    @Override
    public LuceneStorage build() {
      return new LuceneStorage(indexDirectory);
    }
  }

  class LuceneSpanStore implements SpanStore, Traces, ServiceAndSpanNames {
    @Override
    public Call<List<String>> getRemoteServiceNames(String serviceName) {
      return new SupplierCall<>(() -> Collections.emptyList());
    }

    @Override
    public Call<List<List<Span>>> getTraces(QueryRequest request) {
      return new SupplierCall<>(() -> {
        return Collections.emptyList();
      });
    }

    @Override
    public Call<List<Span>> getTrace(String traceId) {
      return new SupplierCall<>(() -> Collections.emptyList());
    }

    @Override
    public Call<List<List<Span>>> getTraces(Iterable<String> traceIds) {
      return new SupplierCall<>(() -> Collections.emptyList());
    }

    @Override
    public Call<List<String>> getServiceNames() {
      return new SupplierCall<>(() -> Collections.emptyList());
    }

    @Override
    public Call<List<String>> getSpanNames(String serviceName) {
      return new SupplierCall<>(() -> Collections.emptyList());
    }

    @Override
    public Call<List<DependencyLink>> getDependencies(long endTs, long lookback) {
      // FIXME implement me!
      return new SupplierCall<>(Collections::emptyList);
    }
  }

  class LuceneSpanConsumer implements SpanConsumer, Closeable {
    private final IndexWriter indexWriter;

    LuceneSpanConsumer(File indexDirectory) {
      try {
        Directory memoryIndex = FSDirectory.open(indexDirectory.toPath());
        StandardAnalyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
        this.indexWriter = new IndexWriter(memoryIndex, indexWriterConfig);
      } catch (IOException e) {
        throw new RuntimeException("Unable to configure Lucene storage module", e);
      }
    }

    private BiFunction<String, Roaring64NavigableMap, Roaring64NavigableMap> addToBitmap(Span span) {
      return (id, bitmap) -> {
        if(bitmap == null) {
          bitmap = new Roaring64NavigableMap();
        }
        bitmap.add(Long.valueOf(span.id(), 16));
        return bitmap;
      };
    }

    @Override
    public Call<Void> accept(List<Span> spans) {
      if (spans.isEmpty()) {
        return Call.create(null);
      }
      return new SupplierCall<>(() -> {
        for (Span span : spans) {
          BiFunction<String, Roaring64NavigableMap, Roaring64NavigableMap> addSpan = addToBitmap(span);
          spanIdsByServiceName.compute(span.localServiceName(), addSpan);
          spanIdsByRemoteServiceName.compute(span.remoteServiceName(), addSpan);
          spanIdsByTraceId.compute(span.traceId(), addSpan);

          Document document = new Document();

          document.add(new TextField("spanId", span.id(), Field.Store.YES));
          document.add(new TextField("serviceName", span.localServiceName(), NO));
          document.add(new TextField("remoteServiceName", span.remoteServiceName(), NO));
          document.add(new LongPoint("timestampPoint", span.timestamp()));
          document.add(new LongPoint("durationPoint", span.duration()));
          document.add(new TextField("annotationsAndTagKeys",
              Streams.concat(
                span.annotations().stream().map(Annotation::value)
              ).collect(joining(" ")),
              NO
            )
          );

          span.tags().entrySet().stream()
            .filter(tag -> StringUtils.isNotBlank(tag.getValue()))
            .forEach(tag -> {
              document.add(new TextField("__" + tag.getKey(), tag.getValue(), NO));
            });

          indexWriter.addDocument(document);
        }
        return null;
      });
    }

    @Override
    public void close() throws IOException {
      indexWriter.close();
    }
  }

  class LuceneAutocompleteTags implements AutocompleteTags {
    @Override
    public Call<List<String>> getKeys() {
      return null;
    }

    @Override
    public Call<List<String>> getValues(String key) {
      return null;
    }
  }

  @Override
  public AutocompleteTags autocompleteTags() {
    return new LuceneAutocompleteTags();
  }

  @Override
  public ServiceAndSpanNames serviceAndSpanNames() {
    return spanStore;
  }

}
