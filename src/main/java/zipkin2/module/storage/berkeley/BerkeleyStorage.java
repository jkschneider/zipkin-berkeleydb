package zipkin2.module.storage.berkeley;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.*;
import zipkin2.Call;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.module.storage.berkeley.model.BerkeleySpan;
import zipkin2.storage.*;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;

public class BerkeleyStorage extends StorageComponent {
  private final EntityStore store;
  private final TimestampBuckets timestampBuckets;

  private final PrimaryIndex<String, BerkeleySpan> spanById;
  private final SecondaryIndex<String, String, BerkeleySpan> spansByTraceId;
  private final SecondaryIndex<String, String, BerkeleySpan> spansBySpanName;
  private final SecondaryIndex<Long, String, BerkeleySpan> spansByTimestampBucket;
  private final SecondaryIndex<String, String, BerkeleySpan> spansByServiceName;
  private final SecondaryIndex<String, String, BerkeleySpan> spansByRemoteServiceName;

  public static Builder newBuilder(File db) {
    return new Builder(db);
  }

  public static final class Builder extends StorageComponent.Builder {
    private final File db;
    private int timestampBucketMinutes = 15;
    private int cachePercent = 65;

    public Builder(File db) {
      this.db = db;
    }

    /** {@inheritDoc} */
    @Override
    public final Builder strictTraceId(boolean strictTraceId) {
      if (!strictTraceId) {
        throw new UnsupportedOperationException("strictTraceId cannot be disabled");
      }
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public final Builder searchEnabled(boolean searchEnabled) {
      if (!searchEnabled) {
        throw new UnsupportedOperationException("searchEnabled cannot be disabled");
      }
      return this;
    }

    public final Builder timestampBucketMinutes(int timestampBucketMinutes) {
      this.timestampBucketMinutes = timestampBucketMinutes;
      return this;
    }

    public final Builder cachePercent(int cachePercent) {
      this.cachePercent = cachePercent;
      return this;
    }

    @Override
    public BerkeleyStorage build() {
      return new BerkeleyStorage(db, timestampBucketMinutes, cachePercent);
    }
  }

  private BerkeleyStorage(File directory, int timestampBucketMinutes, int cachePercent) {
    this.timestampBuckets = new TimestampBuckets(timestampBucketMinutes);
    try {
      EnvironmentConfig envConfig = new EnvironmentConfig();
      envConfig.setAllowCreate(true);
      envConfig.setTransactional(false);
      envConfig.setCachePercent(cachePercent);

      Environment environment = new Environment(directory, envConfig);

      StoreConfig storeConfig = new StoreConfig();
      storeConfig.setReadOnly(false);
      storeConfig.setAllowCreate(true);
      storeConfig.setTransactional(false);

      this.store = new EntityStore(environment, "zipkin", storeConfig);

      this.spanById = store.getPrimaryIndex(String.class, BerkeleySpan.class);
      this.spansByTraceId = store.getSecondaryIndex(spanById, String.class, "traceId");
      this.spansBySpanName = store.getSecondaryIndex(spanById, String.class, "spanName");
      this.spansByTimestampBucket = store.getSecondaryIndex(spanById, Long.class, "timestampBucket");
      this.spansByServiceName = store.getSecondaryIndex(spanById, String.class, "serviceName");
      this.spansByRemoteServiceName = store.getSecondaryIndex(spanById, String.class, "remoteServiceName");
    } catch (DatabaseException e) {
      throw new RuntimeException("Error during BerkeleyJE initialization", e);
    }
  }

  private class BerkeleySpanStore implements SpanStore, Traces, ServiceAndSpanNames {
    @Override
    public Call<List<String>> getRemoteServiceNames(String serviceName) {
      return new SupplierCall<>(() -> stream(spansByRemoteServiceName.keys().spliterator(), false)
        .distinct().collect(toList()));
    }

    @Override
    public Call<List<List<Span>>> getTraces(QueryRequest request) {
      return new SupplierCall<>(() -> {
        Stream<List<BerkeleySpan>> results = IntStream
          .range(0, timestampBuckets.lookbackBucketCount(request.lookback()))
          .mapToLong(n -> timestampBuckets.bucket(request.endTs(), n))
          .mapToObj(bucket -> {
            EntityJoin<String, BerkeleySpan> join = new EntityJoin<>(spanById);

            join.addCondition(spansByTimestampBucket, bucket);

            if (request.serviceName() != null) {
              join.addCondition(spansByServiceName, request.serviceName());
            }

            if (request.spanName() != null) {
              join.addCondition(spansBySpanName, request.spanName());
            }

            if (request.remoteServiceName() != null) {
              join.addCondition(spansByRemoteServiceName, request.remoteServiceName());
            }

            return join.entities();
          })
          .flatMap(spans -> stream(spans.spliterator(), false))
          .limit(request.limit())
          .sorted()
          .collect(groupingBy(BerkeleySpan::getTraceId))
          .values()
          .stream();

        if(!request.annotationQuery().isEmpty()) {
          results = results.filter(trace -> {
            List<String> keyOnly = request.annotationQuery().entrySet().stream()
              .filter(a -> a.getValue() == null || a.getValue().isEmpty())
              .map(Map.Entry::getKey)
              .collect(toList());

            if(trace.stream().noneMatch(span -> span.getAnnotationAndTagKeys().containsAll(keyOnly))) {
              return false;
            }

            List<Map.Entry<String, String>> tags = request.annotationQuery().entrySet().stream()
              .filter(a -> a.getValue() != null)
              .collect(toList());

            return trace.stream().anyMatch(span -> {
              for (Map.Entry<String, String> tag : tags) {
                if(!span.getTags().get(tag.getKey()).equals(tag.getValue())) {
                  return false;
                }
              }
              return true;
            });
          });
        }

        if(request.minDuration() != null) {
          results = results.filter(trace -> {
            long traceDuration = trace.stream().mapToLong(BerkeleySpan::getDuration).sum();
            return traceDuration >= request.minDuration() && request.maxDuration() != null &&
              traceDuration <= request.maxDuration();
          });
        }

        return results
          .map(trace -> trace.stream().map(BerkeleySpan::toSpan).collect(toList()))
          .collect(toList());
      });
    }

    @Override
    public Call<List<Span>> getTrace(String traceId) {
      return new SupplierCall<>(() -> stream(spansByTraceId.subIndex(traceId).entities().spliterator(), false)
        .map(BerkeleySpan::toSpan)
        .sorted()
        .collect(toList()));
    }

    @Override
    public Call<List<List<Span>>> getTraces(Iterable<String> traceIds) {
      return new SupplierCall<>(() ->
        stream(traceIds.spliterator(), false)
          .map(traceId ->
            stream(spansByTraceId.subIndex(traceId).entities().spliterator(), false)
              .sorted()
              .map(BerkeleySpan::toSpan)
              .collect(toList())
          )
          .collect(toList())
      );
    }

    @Override
    public Call<List<String>> getServiceNames() {
      return new SupplierCall<>(() -> stream(spansByServiceName.keys().spliterator(), false)
        .distinct().collect(toList()));
    }

    @Override
    public Call<List<String>> getSpanNames(String serviceName) {
      return new SupplierCall<>(() -> stream(spansBySpanName.keys().spliterator(), false)
        .distinct().collect(toList()));
    }

    @Override
    public Call<List<DependencyLink>> getDependencies(long endTs, long lookback) {
      // FIXME implement me!
      return new SupplierCall<>(Collections::emptyList);
    }
  }

  private class BerkeleySpanConsumer implements SpanConsumer {
    @Override
    public Call<Void> accept(List<Span> spans) {
      if (spans.isEmpty()) {
        return Call.create(null);
      }
      return new SupplierCall<>(() -> {
        for (Span span : spans) {
          spanById.put(BerkeleySpan.fromSpan(span, timestampBuckets));
        }
        return null;
      });
    }
  }

  @Override
  public SpanStore spanStore() {
    return new BerkeleySpanStore();
  }

  @Override
  public SpanConsumer spanConsumer() {
    return new BerkeleySpanConsumer();
  }
}
