package zipkin2.module.storage.lucene;

import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.params.aggregator.ArgumentsAccessor;
import org.junit.jupiter.params.aggregator.ArgumentsAggregator;

import java.util.HashMap;
import java.util.Map;

public class AnnotationQueryAggregator implements ArgumentsAggregator {
  @Override
  public Map<String, String> aggregateArguments(ArgumentsAccessor arguments, ParameterContext context) {
    return arguments.toList().stream()
      .map(String.class::cast)
      .collect(HashMap::new, (m, annot) -> {
        String[] annotParts = annot.split("=");
        m.put(annotParts[0], annotParts.length > 1 ? (annotParts[1].equals("null") ? null : annotParts[1]) : "");
      }, HashMap::putAll);
  }
}