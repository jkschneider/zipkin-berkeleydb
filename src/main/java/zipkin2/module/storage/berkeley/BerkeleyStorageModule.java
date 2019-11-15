package zipkin2.module.storage.berkeley;

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
@EnableConfigurationProperties(BerkeleyStorageProperties.class)
@ConditionalOnProperty(name = "zipkin.storage.type", havingValue = "berkeley")
@ConditionalOnMissingBean(StorageComponent.class)
public class BerkeleyStorageModule {
  @Bean
  StorageComponent berkeleyStorage(@Value("${zipkin.storage.strict-trace-id:true}") boolean strictTraceId,
                                              BerkeleyStorageProperties properties) throws IOException {
    File db = properties.getDbPath() != null ?
      new File(properties.getDbPath()) :
      Files.createTempDirectory("zipkin").toFile();

    if(!db.exists()) {
      db.mkdirs();
    }

    return BerkeleyStorage.newBuilder(db)
      .strictTraceId(strictTraceId)
      .cachePercent(properties.getCachePercent())
      .timestampBucketMinutes(properties.getTimestampBucketMinutes())
      .build();
  }
}
