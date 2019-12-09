package zipkin2.module.storage.lucene;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import zipkin2.storage.StorageComponent;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

@Configuration
@EnableConfigurationProperties(LuceneStorageProperties.class)
@ConditionalOnProperty(name = "zipkin.storage.type", havingValue = "lucene")
@ConditionalOnMissingBean(StorageComponent.class)
public class LuceneStorageModule {
  @Bean
  StorageComponent luceneStorage(@Value("${zipkin.storage.strict-trace-id:true}") boolean strictTraceId,
                                 LuceneStorageProperties properties) throws IOException {
    File index = properties.getIndexPath() != null ?
      new File(properties.getIndexPath()) :
      Files.createTempDirectory("lucene").toFile();

    if (!index.exists()) {
      index.mkdirs();
    }

    return LuceneStorage.newBuilder(index)
      .strictTraceId(strictTraceId)
      .build();
  }
}
