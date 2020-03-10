package no.nav.pdlsf

import java.io.File
import java.io.FileNotFoundException

object ParamsFactory {
    val p: Params by lazy { Params() }
}

// TODO:: Read parameters from vault
data class Params(
        // kafka details
    val kafkaBrokers: String = System.getenv("KAFKA_BROKERS")?.toString() ?: "",
    val kafkaSchemaRegistry: String = System.getenv("KAFKA_SCREG")?.toString() ?: "",
    val kafkaClientID: String = System.getenv("KAFKA_CLIENTID")?.toString() ?: "",
    val kafkaSecurity: String = System.getenv("KAFKA_SECURITY")?.toString()?.toUpperCase() ?: "",
    val kafkaSecProt: String = System.getenv("KAFKA_SECPROT")?.toString() ?: "",
    val kafkaSaslMec: String = System.getenv("KAFKA_SASLMEC")?.toString() ?: "",
    val kafkaUser: String = ("/var/run/secrets/nais.io/serviceuser/username".readFile() ?: "username"),
    val kafkaPassword: String = ("/var/run/secrets/nais.io/serviceuser/password".readFile() ?: "password"),
    val kafkaTopic: String = System.getenv("KAFKA_TOPIC")?.toString() ?: "",

        // salesforce details
    val sfInstType: SalesforceInstancetype = SalesforceInstancetype.valueOf(System.getenv("SF_INSTTYPE") ?: "PREPROD"),
    val sfClientID: String = System.getenv("SF_CLIENTID") ?: "3MVG92H4TjwUcLlJIqjsODbsRUS_SyXMVAuaoZvZJpDwJlf29cO00qGniQv29e2AfJbhHzi5Qb_GxOAUuOUFt",
    val sfUsername: String = System.getenv("SF_USERNAME") ?: "kafka.integrasjon@navtest.no",
        // keystore details
    val ksBase64encoded: String = ("/var/run/secrets/nais.io/vault/keystorejksB64".readFile() ?: getStringFromResource("/keystorejksB64")), //
    val ksPwd: String = System.getenv("KS_PWD") ?: "password",
    val pkAlias: String = System.getenv("PK_ALIAS") ?: "testjwt",
    val pkPwd: String = System.getenv("PK_PWD") ?: "password",
        // other details
    val httpsProxy: String = System.getenv("HTTPS_PROXY") ?: "",
    val msBetweenWork: Long = System.getenv("MS_BETWEEN_WORK")?.toLong() ?: 5 * 60 * 1_000

)

fun Params.sfDetailsComplete(): Boolean = sfClientID.isNotEmpty() && sfUsername.isNotEmpty()

fun Params.kafkaSecurityEnabled(): Boolean = kafkaSecurity == "TRUE"

fun Params.kafkaSecurityComplete(): Boolean =
        kafkaSecProt.isNotEmpty() && kafkaSaslMec.isNotEmpty() && kafkaUser.isNotEmpty() && kafkaPassword.isNotEmpty()

internal fun String.readFile(): String? =
        try {
            File(this).readText(Charsets.UTF_8)
        } catch (err: FileNotFoundException) {
            null
        }

internal fun getStringFromResource(path: String) =
        ParamsFactory::class.java.getResourceAsStream(path).bufferedReader().use { it.readText() }
