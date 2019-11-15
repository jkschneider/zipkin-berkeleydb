package zipkin2.module.storage.berkeley;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("zipkin.storage.berkeley")
public class BerkeleyStorageProperties {
  private String dbPath;
  private int timestampBucketMinutes;
  private int cachePercent;

  public int getTimestampBucketMinutes() {
    return timestampBucketMinutes;
  }

  public void setTimestampBucketMinutes(int timestampBucketMinutes) {
    this.timestampBucketMinutes = timestampBucketMinutes;
  }

  public String getDbPath() {
    return dbPath;
  }

  public void setDbPath(String dbPath) {
    this.dbPath = dbPath;
  }

  public int getCachePercent() {
    return cachePercent;
  }

  public void setCachePercent(int cachePercent) {
    this.cachePercent = cachePercent;
  }
}
