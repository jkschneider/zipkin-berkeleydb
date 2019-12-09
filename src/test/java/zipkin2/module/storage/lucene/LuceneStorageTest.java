package zipkin2.module.storage.lucene;

import com.google.common.collect.ImmutableMap;
import com.sleepycat.persist.EntityCursor;
import org.assertj.core.util.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.module.storage.lucene.model.BerkeleySpan;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static org.assertj.core.api.Assertions.assertThat;

class LuceneStorageTest {
  private LuceneStorage storage;
  private LuceneStorage.BerkeleySpanStore spanStore;
  private LuceneStorage.BerkeleySpanConsumer spanConsumer;

  private List<BerkeleySpan> allSpansAsTrace;

  @BeforeEach
  void before(@TempDir File dbPath) {
    this.storage = LuceneStorage.newBuilder(dbPath).build();
    this.spanStore = (LuceneStorage.BerkeleySpanStore) storage.spanStore();
    this.spanConsumer = (LuceneStorage.BerkeleySpanConsumer) storage.spanConsumer();

    Endpoint endpoint = Endpoint.newBuilder().serviceName("service").build();

    spanConsumer.accept(Arrays.asList(
      Span.newBuilder()
        .id("1")
        .traceId("1")
        .name("http.server.requests")
        .localEndpoint(endpoint)
        .remoteEndpoint(endpoint)
        .addAnnotation(123, "mock.user")
        .putTag("method", "GET")
        .timestamp(1)
        .duration(1)
        .build(),
      Span.newBuilder()
        .id("2")
        .traceId("1")
        .name("http.connection.start")
        .localEndpoint(endpoint)
        .remoteEndpoint(endpoint)
        .addAnnotation(123, "mock.user")
        .timestamp(2)
        .duration(2)
        .build()
    ));

    try(EntityCursor<BerkeleySpan> allSpans = storage.spanById.entities()) {
      this.allSpansAsTrace = stream(allSpans.spliterator(), false).collect(toList());
    }
  }

  @AfterEach
  void after() {
    storage.close();
  }

  @Nested
  class BerkeleySpanStoreTest {
    @SuppressWarnings("unchecked")
    @Test
    void filterAnnotationQuery() {
      List<Map<String, String>> queries = Arrays.asList(
        ImmutableMap.of("mock.user", ""),
        Maps.newHashMap("mock.user", (String) null),
        ImmutableMap.of("mock.user", "", "method", ""),
        ImmutableMap.of("mock.user", "", "method", "GET")
      );

      assertThat(queries)
        .extracting(query -> spanStore.filterAnnotationQuery(Stream.of(allSpansAsTrace), query)
          .findFirst().get())
        .containsExactly(allSpansAsTrace, allSpansAsTrace, allSpansAsTrace, allSpansAsTrace);
    }
  }
}
