package zipkin2.module.storage.lucene;

import com.google.common.collect.ImmutableMap;
import org.assertj.core.util.Maps;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.ServiceAndSpanNames;
import zipkin2.storage.SpanStore;
import zipkin2.storage.Traces;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class LuceneStorageTest {
  private static LuceneStorage storage;
  private static SpanStore spanStore;

  private static Endpoint endpoint = Endpoint.newBuilder().serviceName("service").build();
  private static Endpoint endpoint2 = Endpoint.newBuilder().serviceName("service2").build();

  private static List<Span> trace = Arrays.asList(
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
      .name("http.server.requests")
      .localEndpoint(endpoint2)
      .remoteEndpoint(endpoint2)
      .addAnnotation(123, "mock.user")
      .putTag("method", "GET")
      .timestamp(1)
      .duration(1)
      .build(),
    Span.newBuilder()
      .id("3")
      .traceId("1")
      .name("http.connection.start")
      .localEndpoint(endpoint)
      .remoteEndpoint(endpoint)
      .addAnnotation(123, "mock.user")
      .timestamp(2)
      .duration(2)
      .build()
  );

  @BeforeAll
  static void before(@TempDir File indexDirectory) throws IOException {
    storage = LuceneStorage.newBuilder(indexDirectory).build();
    spanStore = storage.spanStore();
    storage.spanConsumer().accept(trace).execute();
  }

  @AfterAll
  static void after() throws IOException {
    storage.close();
  }

  @Nested
  class LuceneSpanStoreTest {
    @Test
    void getTraceById() throws IOException {
      assertThat(((Traces) spanStore).getTrace("1").execute()).hasSameElementsAs(trace);
    }

    @RepeatedTest(3)
    void spanNamesByServiceName() throws IOException {
      assertThat(((ServiceAndSpanNames) spanStore).getSpanNames("service").execute())
        .containsExactlyInAnyOrder("http.server.requests", "http.connection.start");
    }

    @SuppressWarnings("unchecked")
    @Test
    void filterAnnotationQuery() {
      List<Map<String, String>> queries = Arrays.asList(
        Maps.newHashMap("mock.user", ""),
        Maps.newHashMap("mock.user", null),
        ImmutableMap.of("mock.user", "", "method", ""),
        ImmutableMap.of("mock.user", "", "method", "GET")
      );

      assertThat(queries)
        .extracting(query -> spanStore.getTraces(QueryRequest.newBuilder()
          .annotationQuery(query)
          .endTs(2)
          .lookback(2)
          .limit(1)
          .build()).execute())
        .extracting(traces -> traces.stream().findFirst().orElse(null))
        .containsExactly(trace, trace, trace, trace);
    }
  }
}
