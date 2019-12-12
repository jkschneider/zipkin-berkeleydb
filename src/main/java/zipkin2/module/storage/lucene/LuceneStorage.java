package zipkin2.module.storage.lucene;

import com.google.common.collect.Streams;
import io.micrometer.core.instrument.util.StringUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.grouping.DistinctValuesCollector;
import org.apache.lucene.search.grouping.FirstPassGroupingCollector;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.grouping.TermGroupSelector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import zipkin2.Annotation;
import zipkin2.Call;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.storage.AutocompleteTags;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.ServiceAndSpanNames;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.SpanStore;
import zipkin2.storage.StorageComponent;
import zipkin2.storage.Traces;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static org.apache.lucene.document.Field.Store.NO;
import static org.apache.lucene.search.BooleanClause.Occur.MUST;

public class LuceneStorage extends StorageComponent {
  private final LuceneSpanConsumer spanConsumer;
  private final LuceneSpanStore spanStore;

  private final DB db;
  private final HTreeMap<String, Roaring64NavigableMap> traceIdsByServiceName;
  private final HTreeMap<String, Roaring64NavigableMap> traceIdsByRemoteServiceName;
  private final HTreeMap<String, Roaring64NavigableMap> spanIdsByTraceId;
  private final HTreeMap<UniqueSpanId, Span> spanBySpanId;

  private final IndexWriter indexWriter;

  private LuceneStorage(File indexDirectory) {
    try {
      Directory fileIndex = FSDirectory.open(new File(indexDirectory, "lucene").toPath());
      StandardAnalyzer analyzer = new StandardAnalyzer();
      IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
      this.indexWriter = new IndexWriter(fileIndex, indexWriterConfig);

      this.spanConsumer = new LuceneSpanConsumer();
      this.spanStore = new LuceneSpanStore();
    } catch(IOException e) {
      throw new RuntimeException("Unable to configure Lucene storage module", e);
    }

    this.db = DBMaker
      .fileDB(new File(indexDirectory, "map.db"))
      .closeOnJvmShutdown()
      .fileMmapEnableIfSupported()
      .make();

    this.spanIdsByTraceId = searchByCriteriaMap("traceId");
    this.traceIdsByServiceName = searchByCriteriaMap("serviceName");
    this.traceIdsByRemoteServiceName = searchByCriteriaMap("remoteServiceName");
    this.spanBySpanId = db.hashMap("spanId")
      .keySerializer(UniqueSpanId.SERIALIZER)
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
    this.indexWriter.close();
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

  class LuceneSpanStore implements Traces, SpanStore, ServiceAndSpanNames {
    @Override
    public Call<List<String>> getRemoteServiceNames(String serviceName) {
      return new SupplierCall<>(() -> traceIdsByRemoteServiceName.getKeys().stream().sorted().collect(toList()));
    }

    @Override
    public Call<List<Span>> getTrace(String traceId) {
      return new SupplierCall<>(() -> getTraceBlocking(traceId));
    }

    @Override
    public Call<List<List<Span>>> getTraces(Iterable<String> traceIds) {
      return new SupplierCall<>(() -> stream(traceIds.spliterator(), false)
        .map(this::getTraceBlocking)
        .collect(toList()));
    }

    private List<Span> getTraceBlocking(String traceId) {
      String normalizedTraceId = Span.normalizeTraceId(traceId);
      Roaring64NavigableMap bitmap = spanIdsByTraceId.get(normalizedTraceId);
      if(bitmap == null)
        return emptyList();

      List<Span> spans = new ArrayList<>();
      bitmap.forEach(spanId -> {
        String normalizedSpanId = Span.normalizeTraceId(Long.toHexString(spanId));
        spans.add(spanBySpanId.get(new UniqueSpanId(normalizedTraceId, normalizedSpanId)));
      });

      return spans;
    }

    @Override
    public Call<List<String>> getServiceNames() {
      return new SupplierCall<>(() -> traceIdsByServiceName.keySet().stream().sorted().collect(toList()));
    }

    @Override
    public Call<List<String>> getSpanNames(String serviceName) {
      return new SupplierCall<>(() -> {
        try(IndexReader reader = DirectoryReader.open(indexWriter)) {
          TermGroupSelector spanNameGroupSelector = new TermGroupSelector("spanName");
          FirstPassGroupingCollector<BytesRef> firstPassCollector = new FirstPassGroupingCollector<>(spanNameGroupSelector,
            Sort.INDEXORDER, 10000);

          new IndexSearcher(reader).search(new TermQuery(new Term("serviceName", serviceName)), firstPassCollector);

          Collection<SearchGroup<BytesRef>> topGroups = firstPassCollector.getTopGroups(0);
          if(topGroups == null) {
            return emptyList();
          }

          return new DistinctValuesCollector<>(spanNameGroupSelector, topGroups, spanNameGroupSelector)
            .getGroups()
            .stream()
            .map(group -> group.groupValue.utf8ToString())
            .collect(toList());
        }
      });
    }

    @Override
    public Call<List<DependencyLink>> getDependencies(long endTs, long lookback) {
      // FIXME implement me!
      return new SupplierCall<>(Collections::emptyList);
    }

    @Override
    public Call<List<List<Span>>> getTraces(QueryRequest request) {
      return new SupplierCall<>(() -> {
        try(IndexReader reader = DirectoryReader.open(indexWriter)) {
          BooleanQuery.Builder query = new BooleanQuery.Builder();

          if (request.remoteServiceName() != null) {
            query.add(new TermQuery(new Term("remoteServiceName", request.remoteServiceName())), MUST);
          }

          if (request.serviceName() != null) {
            query.add(new TermQuery(new Term("serviceName", request.serviceName())), MUST);
          }

          if (request.spanName() != null) {
            query.add(new TermQuery(new Term("spanName", request.spanName())), MUST);
          }

          for (Map.Entry<String, String> annotation : request.annotationQuery().entrySet()) {
            if(annotation.getValue() != null) {
              query.add(new TermQuery(new Term("__" + annotation.getKey(), annotation.getValue())), MUST);
            }
          }

          IndexSearcher indexSearcher = new IndexSearcher(reader);
          TopDocs search = indexSearcher.search(query.build(), request.limit());

          List<List<Span>> traces = new ArrayList<>();
          for (ScoreDoc scoreDoc : search.scoreDocs) {
            Document doc = indexSearcher.doc(scoreDoc.doc);
            traces.add(getTraceBlocking(doc.getField("traceId").stringValue()));
          }

          return traces;
        }
      });
    }
  }

  class LuceneSpanConsumer implements SpanConsumer {
    @Override
    public Call<Void> accept(List<Span> spans) {
      if (spans.isEmpty()) {
        return Call.create(null);
      }
      return new SupplierCall<>(() -> {
        for (Span span : spans) {
          addToBitmap(traceIdsByServiceName, span.localServiceName(), span.traceId());
          addToBitmap(traceIdsByRemoteServiceName, span.remoteServiceName(), span.traceId());
          addToBitmap(spanIdsByTraceId, span.traceId(), span.id());
          spanBySpanId.put(new UniqueSpanId(span.traceId(), span.id()), span);

          Document document = new Document();

          document.add(new TextField("traceId", span.traceId(), Field.Store.YES));
          document.add(new TextField("spanId", span.id(), Field.Store.YES));
          document.add(new SortedDocValuesField("spanName", new BytesRef(span.name())));
//          document.add(new StoredField("spanName", span.name()));
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
            .forEach(tag -> document.add(new TextField("__" + tag.getKey(), tag.getValue(), NO)));

          indexWriter.addDocument(document);
        }
        indexWriter.commit();
        return null;
      });
    }

    private void addToBitmap(HTreeMap<String, Roaring64NavigableMap> map, String key, String value) {
      Roaring64NavigableMap bitmap = map.get(key);
      if(bitmap == null) {
        bitmap = new Roaring64NavigableMap();
      }
      bitmap.add(Long.valueOf(value, 16));
      map.put(key, bitmap);
    }
  }

  static class LuceneAutocompleteTags implements AutocompleteTags {
    @Override
    public Call<List<String>> getKeys() {
      // FIXME implement me!
      return null;
    }

    @Override
    public Call<List<String>> getValues(String key) {
      // FIXME implement me!
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
