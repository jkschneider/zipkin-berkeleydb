package zipkin2.module.storage.berkeley;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static org.assertj.core.api.Assertions.assertThat;

class TimestampBucketsTest {
  private final TimestampBuckets buckets = new TimestampBuckets(15);
  private final DateTimeFormatter minutes = DateTimeFormatter.ofPattern("m");

  @Test
  void timestampBucket() {
    long ts = 1573850593;
    for(int i = 0; i < 1000; i++) {
      ts += 37*i;
      long bucket = buckets.bucket(ts);
      assertThat(minutes.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(bucket), ZoneId.systemDefault())))
        .isIn("0", "15", "30", "45");
    }
  }

  @Test
  void lookbackBucketCount() {
    assertThat(buckets.lookbackBucketCount(100)).isEqualTo(1);
    assertThat(buckets.lookbackBucketCount(900000)).isEqualTo(1);
    assertThat(buckets.lookbackBucketCount(900001)).isEqualTo(2);
  }
}