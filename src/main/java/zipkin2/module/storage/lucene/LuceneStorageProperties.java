package zipkin2.module.storage.lucene;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("zipkin.storage.lucene")
public class LuceneStorageProperties {
  private String indexPath = ".lucene";

  public String getIndexPath() {
    return indexPath;
  }

  public void setIndexPath(String indexPath) {
    this.indexPath = indexPath;
  }
}
