package no.nav.pdlsf

import java.security.KeyStore
import java.security.PrivateKey
import java.security.Signature
import kotlinx.serialization.Serializable
import mu.KotlinLogging
import org.http4k.core.Method
import org.http4k.core.Request
import org.http4k.core.Status

private val log = KotlinLogging.logger { }

@Serializable
internal data class JWTHeader(val alg: String = "RS256") {
    fun toJson(): String = json.stringify(serializer(), this)
}

@Serializable
internal data class JWTClaimSet(
    val iss: String = ParamsFactory.p.sfClientID,
    val aud: String = ParamsFactory.p.sfInstType.url,
    val sub: String = ParamsFactory.p.sfUsername,
    val exp: String = ((System.currentTimeMillis() / 1000) + 300).toString()
) {
    fun toJson(): String = json.stringify(serializer(), this)
}

internal fun getHeaderClaimset(cs: JWTClaimSet): String = JWTHeader().toJson().encodeB64() + "." + cs.toJson().encodeB64()

private data class KeystoreDetails(
    val ksBase64encoded: String = ParamsFactory.p.ksBase64encoded,
    val pwd: String = ParamsFactory.p.ksPwd,
    val pkAlias: String = ParamsFactory.p.pkAlias,
    val pkPwd: String = ParamsFactory.p.pkPwd
)

fun ByteArray.encodeB64(): String = org.apache.commons.codec.binary.Base64.encodeBase64URLSafeString(this)
fun String.encodeB64(): String = org.apache.commons.codec.binary.Base64.encodeBase64URLSafeString(this.toByteArray())
fun String.decodeB64(): ByteArray = org.apache.commons.codec.binary.Base64.decodeBase64(this)
private sealed class SignatureBase
private object MissingSignature : SignatureBase()
private data class Signature(val content: String) : SignatureBase()

private fun KeystoreDetails.sign(data: ByteArray): SignatureBase = runCatching {
    KeyStore.getInstance("JKS")
            .apply {
                load(ksBase64encoded.decodeB64().inputStream(), pwd.toCharArray())
            }
            .run { getKey(pkAlias, pkPwd.toCharArray()) as PrivateKey }
            .let { privateKey ->
                Signature.getInstance("SHA256withRSA")
                        .apply {
                            initSign(privateKey)
                            update(data)
                        }
                        .run { Signature(sign().encodeB64()) }
            }
}
        .onFailure { log.error { "Signing data failed - ${it.localizedMessage}" } }
        .getOrDefault(MissingSignature)
internal sealed class AuthorizationBase
internal object MissingAuthorization : AuthorizationBase()

@Serializable
internal data class Authorization(
    val access_token: String = "",
    val scope: String = "",
    val instance_url: String = "",
    val id: String = "",
    val token_type: String = ""
) : AuthorizationBase() {
    companion object {
        fun fromJson(data: String): AuthorizationBase = runCatching { json.parse(serializer(), data) }
                .onFailure { "Parsing of authorization response failed - ${it.localizedMessage}" }
                .getOrDefault(MissingAuthorization)
    }
}

internal fun authorize(): AuthorizationBase = JWTClaimSet().let { cs ->

    fun no.nav.pdlsf.Signature.getAuthByJWT(): AuthorizationBase = Http.client.invoke(
            Request(Method.POST, ParamsFactory.p.sfInstType.oAuthEndpoint())
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .query("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")
                    .query("assertion", getHeaderClaimset(cs) + "." + this.content)
                    .body("")
    ).let { response ->
        when (response.status) {
            Status.OK -> Authorization.fromJson(response.bodyString())
            else -> {
                log.error { "Authorization request failed - ${response.status.description}(${response.status.code})" }
                MissingAuthorization
            }
        }
    }

    when (val signature = KeystoreDetails().sign(getHeaderClaimset(cs).toByteArray())) {
        is MissingSignature -> MissingAuthorization
        is no.nav.pdlsf.Signature -> {
            log.info { "Signature completed, ready for authentication by JWT" }
            signature.getAuthByJWT()
        }
    }
}

internal fun doAuthorization(doSomething: (Authorization) -> Unit): Boolean = when (val authorization = authorize()) {
    is MissingAuthorization -> false
    is Authorization -> {
        doSomething(authorization)
        true
    }
}
