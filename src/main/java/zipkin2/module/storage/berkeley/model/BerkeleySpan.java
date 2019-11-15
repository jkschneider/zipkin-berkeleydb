package zipkin2.module.storage.berkeley.model;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.SecondaryKey;
import zipkin2.Annotation;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.module.storage.berkeley.TimestampBuckets;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.sleepycat.persist.model.Relationship.MANY_TO_ONE;
import static java.util.stream.Collectors.toList;

@Entity
public class BerkeleySpan implements Comparable<BerkeleySpan> {
  @PrimaryKey
  private String spanId;

  @SecondaryKey(relate = MANY_TO_ONE)
  private String traceId;

  @SecondaryKey(relate = MANY_TO_ONE)
  private String spanName;

  private Long timestamp;

  @SecondaryKey(relate = MANY_TO_ONE)
  private String serviceName;

  @SecondaryKey(relate = MANY_TO_ONE)
  private String remoteServiceName;

  private Long duration;

  private Set<String> annotationAndTagKeys = new HashSet<>();

  private List<String> annotations;
  private Map<String, String> tags;

  @SecondaryKey(relate = MANY_TO_ONE)
  private Long timestampBucket;

  public BerkeleySpan() {}

  public Span toSpan() {
    Span.Builder span = Span.newBuilder()
      .id(spanId)
      .traceId(traceId)
      .name(spanName)
      .timestamp(timestamp)
      .duration(duration)
      .localEndpoint(Endpoint.newBuilder()
        .serviceName(serviceName)
        .build())
      .remoteEndpoint(Endpoint.newBuilder()
        .serviceName(remoteServiceName)
        .build());

    for (Map.Entry<String, String> tag : tags.entrySet()) {
      span.putTag(tag.getKey(), tag.getValue());
    }

    for (String annotation : annotations) {
      String[] annotParts = annotation.split("=");
      span.addAnnotation(Long.parseLong(annotParts[1]), annotParts[0]);
    }

    return span.build();
  }

  public static BerkeleySpan fromSpan(Span span, TimestampBuckets timestampBuckets) {
    BerkeleySpan berkeleySpan = new BerkeleySpan();
    berkeleySpan.spanId = span.id();
    berkeleySpan.traceId = span.traceId();
    berkeleySpan.spanName = span.name();
    berkeleySpan.remoteServiceName = span.remoteServiceName();
    berkeleySpan.serviceName = span.localServiceName();
    berkeleySpan.timestamp = span.timestamp();
    berkeleySpan.duration = span.duration();
    berkeleySpan.annotations = span.annotations().stream()
      .map(a -> a.value() + "=" + a.timestamp()).collect(toList());
    berkeleySpan.tags = span.tags();

    span.annotations().stream().map(Annotation::value).forEach(berkeleySpan.annotationAndTagKeys::add);
    berkeleySpan.annotationAndTagKeys.addAll(span.tags().keySet());

    berkeleySpan.timestampBucket = timestampBuckets.bucket(span.timestamp() / 1000);

    return berkeleySpan;
  }

  public String getTraceId() {
    return traceId;
  }

  public Set<String> getAnnotationAndTagKeys() {
    return annotationAndTagKeys;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public Long getDuration() {
    return duration;
  }

  @Override
  public int compareTo(BerkeleySpan s) {
    return (int) (timestamp - s.timestamp);
  }
}