package zipkin2.autoconfigure.reporter.datadog;

import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cloud.sleuth.zipkin2.ZipkinAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import zipkin2.Span;
import zipkin2.reporter.Reporter;
import zipkin2.reporter.datadog.DatadogReporter;

/**
 * Autoconfiguration class to provide a DatadogReporter for zipkin if one hasn't already been
 * provided.
 */
@Configuration
// Since we aren't configuring a sender, this should be detected before AsyncReporter is configured.
@AutoConfigureBefore(ZipkinAutoConfiguration.class)
@ConditionalOnMissingBean(Reporter.class)
public class DatadogReporterAutoConfiguration {

  // Using the standard name "reporter" prevents ZipkinAutoConfiguration
  // from configuring a bean with the same name.
  @Bean
  public Reporter<Span> reporter() {
    return new DatadogReporter();
  }
}
