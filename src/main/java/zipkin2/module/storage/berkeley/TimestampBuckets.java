package zipkin2.module.storage.berkeley;

public class TimestampBuckets {
  private final int bucketWidthSeconds;

  public TimestampBuckets(int bucketWidthMinutes) {
    this.bucketWidthSeconds = (bucketWidthMinutes * 60);
  }

  public long bucket(long ts) {
    return ts / 1000 / bucketWidthSeconds;
  }

  public int lookbackBucketCount(long lookbackMillis) {
    return (int) Math.ceil ((double) lookbackMillis / 1000.0 / bucketWidthSeconds);
  }

  public long bucket(long ts, int numBucketsLeftOfTs) {
    return bucket(ts) - (numBucketsLeftOfTs * bucketWidthSeconds);
  }
}
