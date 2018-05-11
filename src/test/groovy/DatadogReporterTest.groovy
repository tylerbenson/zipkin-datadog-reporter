import spock.lang.Specification
import zipkin2.Endpoint
import zipkin2.Span
import zipkin2.reporter.datadog.DDMappingSpan
import zipkin2.reporter.datadog.DatadogReporter

import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicLong

class DatadogReporterTest extends Specification {
  AtomicLong timestamp = new AtomicLong();
  BlockingQueue<List<DDMappingSpan>> reported = new LinkedBlockingQueue<>();
  DatadogReporter reporter = new DatadogReporter(reported) {
    @Override
    protected long nanoTime() {
      return timestamp.get();
    }
  };

  def "test reporting"() {
    when:
    timestamp.set(1);
    def client = newSpan("a", "b", "c", Span.Kind.CLIENT, "get /backend")
    reporter.report(client);
    timestamp.set(2);
    def serverF = newSpan("f", null, "f", Span.Kind.SERVER, "get /other")
    reporter.report(serverF);

    reporter.flush();

    then:
    reported.each { it.delegateSpan }.asList() == []

    when:
    timestamp.set(3 + DatadogReporter.COMPLETION_DELAY);
    reporter.flush();

    then:
    reported.collect { it.delegateSpan }.asList() == [[serverF]]

    when:
    timestamp.set(4);
    def childB = newSpan("a", "a", "b", null, "callBackend")
    reporter.report(childB);
    timestamp.set(5);
    def serverA = newSpan("a", null, "a", Span.Kind.SERVER, "get /frontend")
    reporter.report(serverA);

    timestamp.set(6 + DatadogReporter.COMPLETION_DELAY);
    reporter.flush();

    then:
    reported.collect { it.delegateSpan }.asList() == [[serverF], [client, childB, serverA]]

    when:
    timestamp.set(7);
    def childE = newSpan("a", "a", "e", null, "lateCall")
    reporter.report(childE);

    timestamp.set(8 + DatadogReporter.COMPLETION_DELAY);
    reporter.flush();

    then:
    reported.collect { it.delegateSpan }.asList() == [[serverF], [client, childB, serverA]]

    when:
    timestamp.set(8 + DatadogReporter.TIMEOUT_DELAY);
    reporter.flush();

    then:
    reported.collect { it.delegateSpan }.asList() == [[serverF], [client, childB, serverA], [childE]]
  }


  static Span newSpan(String traceId, String parentId, String id, Span.Kind kind, String spanName) {
    Span.Builder result = Span.newBuilder().traceId(traceId).parentId(parentId).id(id).kind(kind).name(spanName).localEndpoint(Endpoint.newBuilder().serviceName("my-app").build());
    return result.build();
  }
}
