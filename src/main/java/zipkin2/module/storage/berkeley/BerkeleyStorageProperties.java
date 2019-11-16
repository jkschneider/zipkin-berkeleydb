package zipkin2.module.storage.berkeley;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("zipkin.storage.berkeley")
public class BerkeleyStorageProperties {
  private String dbPath = ".berkeley";
  private int timestampBucketMinutes = 15;
  private int cachePercent = 65;

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
