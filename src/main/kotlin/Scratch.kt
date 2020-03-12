import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonPrimitive
import mu.KotlinLogging
import no.nav.pdlsf.json
import java.io.File

private val log = KotlinLogging.logger { }

sealed class FnrBase
object NoFnr : FnrBase()
data class Fnr(val s: String) : FnrBase()

fun JsonElement.getFnr(): FnrBase = runCatching {
    val fnr = jsonObject.content["hentIdenter"]?.let { hi ->
        hi.jsonObject.content["identer"]?.let { ids ->
            ids.jsonArray.content.first {
                it.jsonObject.content.containsValue(JsonPrimitive("FOLKEREGISTERIDENT")) &&
                        it.jsonObject.content.containsValue(JsonPrimitive(false))
            }
                    .jsonObject
                    .content["ident"]
        }
    }
    fnr?.let { Fnr(fnr.toString()) } ?: NoFnr
}
        .onFailure { log.info { "Failure - ${it.localizedMessage}" }  }
        .getOrDefault(NoFnr)

fun main() {

    val person = json.parseJson(File("./src/test/resources/syntetiskPerson_1.json").readText())

    println(person.getFnr())

}
