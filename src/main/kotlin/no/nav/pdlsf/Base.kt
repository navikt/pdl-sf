package no.nav.pdlsf
import java.net.URI
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonConfiguration
import org.apache.http.HttpHost
import org.apache.http.client.config.CookieSpecs
import org.apache.http.client.config.RequestConfig
import org.apache.http.impl.client.HttpClients
import org.http4k.client.ApacheClient
import org.http4k.core.HttpHandler

internal val json = Json(JsonConfiguration.Stable)

enum class SalesforceInstancetype(
    val url: String,
    val version: String
) {
    PREPROD("https://test.salesforce.com", System.getenv("SF_VERSION") ?: "v48.0"),
    PRODUCTION("https://login.salesforce.com", System.getenv("SF_VERSION") ?: "v48.0")
}

fun SalesforceInstancetype.oAuthEndpoint() = "$url/services/oauth2/token"

object Http {
    val client: HttpHandler by lazy { ApacheClient.proxy() }
}

fun ApacheClient.proxy(): HttpHandler = when {
    ParamsFactory.p.httpsProxy.isEmpty() -> this()
    else -> {
        val up = URI(ParamsFactory.p.httpsProxy)
        this(client =
        HttpClients.custom()
                .setDefaultRequestConfig(
                        RequestConfig.custom()
                                .setProxy(HttpHost(up.host, up.port, up.scheme))
                                .setRedirectsEnabled(false)
                                .setCookieSpec(CookieSpecs.IGNORE_COOKIES)
                                .build())
                .build()
        )
    }
}
