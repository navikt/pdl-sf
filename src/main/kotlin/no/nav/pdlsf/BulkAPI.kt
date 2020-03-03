package no.nav.pdlsf

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import mu.KotlinLogging
import org.http4k.core.Method
import org.http4k.core.Request
import org.http4k.core.Status

private val log = KotlinLogging.logger { }

@Serializable
enum class ColumnDelimiter {
    BACKQUOTE,
    CARET,
    COMMA,
    PIPE,
    SEMICOLON,
    TAB
}

@Serializable
enum class LineEnding {
    LF,
    CRLF
}

@Serializable
enum class Operation {
    @SerialName("insert") INSERT,
    @SerialName("delete") DELETE,
    @SerialName("update") UPDATE,
    @SerialName("upsert") UPSERT
}

@Serializable
data class JobSpecification(
    val columnDelimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
    val contentType: String = "CSV",
    val externalIdFieldName: String = "", // only for upsert operations
    val lineEnding: LineEnding = LineEnding.LF,
    @SerialName("object") val obj: String,
    val operation: Operation
) {
    fun toJson() = json.stringify(serializer(), this)
}

@Serializable
enum class JobType {
    @SerialName("BigObjectIngest") BIGOBJECTINGEST,
    @SerialName("Classic") CLASSIC,
    @SerialName("V2Ingest") V2INGEST,
}

@Serializable
enum class JobState {
    @SerialName("Open") OPEN,
    @SerialName("UploadComplete") UPLOADCOMPLETE,
    @SerialName("Aborted") ABORTED,
    @SerialName("JobComplete") JOBCOMPLETE,
    @SerialName("Failed") FAILED
}

sealed class JobResultBase
object MissingJobResult : JobResultBase()

@Serializable
data class JobResult(
    val apiVersion: String,
    val columnDelimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
    val concurrencyMode: String, // only parallel supported by Salesforce
    val contentType: String, // only CSV supported
    val contentUrl: String = "",
    val createdById: String, // not so relevant
    val createdDate: String, // not so relevant
    val externalIdFieldName: String = "", // in case of upsert
    val id: String, // unique id for the job
    val jobType: JobType = JobType.V2INGEST,
    val lineEnding: LineEnding = LineEnding.LF,
    @SerialName("object") val obj: String,
    val operation: Operation,
    val state: JobState,
    val systemModstamp: String
) : JobResultBase() { companion object { fun fromJson(data: String): JobResultBase = runCatching { json.parse(serializer(), data) }
        .onFailure { log.error { "Parsing of job creation response failed - ${it.localizedMessage}" } }
        .getOrDefault(MissingJobResult) }
}

internal fun Authorization.jobCreationEndpoint() = "$instance_url/services/data/${ParamsFactory.p.sfInstType.version}/jobs/ingest"

internal fun Authorization.createJob(
    aJob: JobSpecification,
    doBatch: ((String) -> Boolean) -> Boolean
): Boolean = runCatching {

    fun Authorization.createJobRequest(): Request = Request(
            Method.POST, jobCreationEndpoint())
            .header("Authorization", "$token_type $access_token")
            .header("Content-Type", "application/json;charset=UTF-8")

    log.info { "Creating bulk job - ${aJob.toJson()}" }

    val responseTime = Metrics.responseLatency.startTimer()
    val response = Http.client.invoke(createJobRequest().body(aJob.toJson()))
            .also { responseTime.observeDuration() }

    when (response.status) {
        Status.OK -> {
            Metrics.successfulRequest.inc()
            when (val jobResult = JobResult.fromJson(response.bodyString())) {
                is MissingJobResult -> false
                is JobResult -> {
                    log.info { "Job id ${jobResult.id} created, status ${jobResult.state}" }
                    jobResult.manageJob(this, doBatch)
                }
            }
        }
        else -> {
            log.error { "Job creation failed - ${response.status.description} - ${response.bodyString()}" }
            false
        }
    }
}
        .onFailure { log.error { it.localizedMessage } }
        .getOrDefault(false)

internal fun JobResult.manageJob(
    auth: Authorization,
    doBatch: ((String) -> Boolean) -> Boolean
): Boolean = runCatching {

    fun JobResult.jobStateEndpoint(auth: Authorization) = "${auth.jobCreationEndpoint()}/$id"

    fun JobResult.changeJobState(state: JobState): JobResultBase = JobResult.fromJson(
            Http.client.invoke(
                    Request(Method.PATCH, jobStateEndpoint(auth))
                            .header("Authorization", "${auth.token_type} ${auth.access_token}")
                            .header("Content-Type", "application/json;charset=UTF-8")
                            .body("""{"state":${json.stringify(JobState.serializer(),state)}}""")
            ).bodyString()
    )

    fun JobResult.abortJob() {
        when (changeJobState(JobState.ABORTED)) {
            is MissingJobResult -> log.error { "Failed to abort job $id - batch will not be executed due to job state" }
            is JobResult -> log.info { "Job $id is aborted, observe mix of processed and cancelled records" }
        }
    }
    fun JobResult.closeJob() {
        when (changeJobState(JobState.UPLOADCOMPLETE)) {
            is MissingJobResult -> log.error { "Failed to close job $id - batch will not be executed!" }
            is JobResult -> log.info { "Job $id is closed, batch will be processed" }
        }
    }

    fun JobResult.completeCSVBatch(csvData: String): Boolean =
            Http.client.invoke(
                    Request(Method.PUT, "${auth.instance_url}/$contentUrl")
                            .header("Authorization", "${auth.token_type} ${auth.access_token}")
                            .header("Content-Type", "text/csv")
                            .body(csvData)
            )
                    .also { log.info { "Added batch to job $id with status ${it.status.description}" } }
                    .status == Status.CREATED

    doBatch(::completeCSVBatch).also { batchSuccess -> if (batchSuccess) closeJob() else abortJob() }
}
        .onFailure { log.error { it.localizedMessage } }
        .getOrDefault(false)
