package no.nav.pdlsf

import io.prometheus.client.CollectorRegistry
import io.prometheus.client.Gauge
import io.prometheus.client.Histogram
import io.prometheus.client.hotspot.DefaultExports
import mu.KotlinLogging

object Metrics {

    private val log = KotlinLogging.logger { }

    val cRegistry: CollectorRegistry = CollectorRegistry.defaultRegistry

    val responseLatency: Histogram = Histogram
        .build()
        .name("response_latency_seconds_histogram")
        .help("Salesforce post response latency")
        .register()

    val successfulRequest: Gauge = Gauge
        .build()
        .name("successful_request_gauge")
        .help("No. of successful requests to Salesforce since last restart")
        .register()

    val invalidQuery: Gauge = Gauge
            .build()
            .name("invalid_query_gauge")
            .help("No. of failed kafka values converted to query on topic since last restart")
            .register()

    val sucessfulValueToQuery: Gauge = Gauge
            .build()
            .name("sucessfully_value_to_query_gauge")
            .help("No of sucessfully converted kafka topic values to query")
            .register()

    init {
        DefaultExports.initialize()
        log.info { "Prometheus metrics are ready" }
    }

    fun sessionReset() {
        invalidQuery.clear()
        sucessfulValueToQuery.clear()
    }

    fun resetAll() {
        invalidQuery.clear()
        sucessfulValueToQuery.clear()
    }
}
