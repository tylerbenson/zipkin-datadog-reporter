package zipkin2.autoconfigure.reporter.datadog;

import java.util.HashMap;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertySource;

/**
 * We use this processor to ensure that spans on the client and server side of a request have
 * distinct IDs.
 */
public class DDEnvironmentPostProcessor implements EnvironmentPostProcessor {

  private static final String PROPERTY_SOURCE_NAME = "defaultProperties";

  @Override
  public void postProcessEnvironment(
      ConfigurableEnvironment environment, SpringApplication application) {
    MutablePropertySources propertySources = environment.getPropertySources();
    MapPropertySource target = null;
    if (propertySources.contains(PROPERTY_SOURCE_NAME)) {
      PropertySource<?> source = propertySources.get(PROPERTY_SOURCE_NAME);
      if (source instanceof MapPropertySource) {
        target = (MapPropertySource) source;
      }
    }
    if (target == null) {
      target = new MapPropertySource(PROPERTY_SOURCE_NAME, new HashMap<>());
    }

    target.getSource().put("spring.sleuth.supports-join", false);

    if (!propertySources.contains(PROPERTY_SOURCE_NAME)) {
      propertySources.addLast(target);
    }
  }
}
