package zipkin2.module.storage.lucene;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.aggregator.AggregateWith;
import org.junit.jupiter.params.provider.CsvSource;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.storage.*;

import java.io.File;
import java.io.IOException;
import java.util.*;

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

    @Test
    void filterSpanName() throws IOException {
      assertThat(
        spanStore.getTraces(QueryRequest.newBuilder()
          .spanName("Http.Server.Requests")
          .endTs(2).lookback(2).limit(1)
          .build()
        ).execute()
      ).containsExactly(trace);
    }

    @Test
    void filterServiceName() throws IOException {
      assertThat(
        spanStore.getTraces(QueryRequest.newBuilder()
          .serviceName("Service")
          .endTs(2).lookback(2).limit(1)
          .build()
        ).execute()
      ).containsExactly(trace);
    }

    @Test
    void filterRemoteServiceName() throws IOException {
      assertThat(
        spanStore.getTraces(QueryRequest.newBuilder()
          .remoteServiceName("Service")
          .endTs(2).lookback(2).limit(1)
          .build()
        ).execute()
      ).containsExactly(trace);
    }

    @ParameterizedTest
    @CsvSource({
      "mock.user",
      "mock.user=null",
      "mock.user, method",
      "mock.User, method=GET"
    })
    void filterAnnotationQuery(@AggregateWith(AnnotationQueryAggregator.class) Map<String, String> query) throws IOException {
      assertThat(
        spanStore.getTraces(QueryRequest.newBuilder()
          .annotationQuery(query)
          .endTs(2).lookback(2).limit(1)
          .build()
        ).execute()
      ).containsExactly(trace);
    }
  }

  @Test
  void normalizeQueryTerms() {
    assertThat(LuceneStorage.normalizeValue("GET")).isEqualTo("get");
    assertThat(LuceneStorage.normalizeValue("HELLO WORLD")).isEqualTo("hello world");
  }
}
