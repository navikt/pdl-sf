import java.io.File
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonLiteral
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.parse
import mu.KotlinLogging
import no.nav.pdlsf.AccountMessage
import no.nav.pdlsf.InvalidQuery
import no.nav.pdlsf.NoSalesforceObject
import no.nav.pdlsf.Person
import no.nav.pdlsf.PersonCMessage
import no.nav.pdlsf.Query
import no.nav.pdlsf.SalesforceObject
import no.nav.pdlsf.SalesforceObjectBase
import no.nav.pdlsf.createAccountMessage
import no.nav.pdlsf.createPersonCMessage
import no.nav.pdlsf.getQueryFromJson
import no.nav.pdlsf.json
import org.apache.kafka.clients.consumer.ConsumerRecord

private val log = KotlinLogging.logger { }

fun JsonElement.getTopicValuePair(): ConsumerRecord<String, String> = runCatching {
    val content = (this as JsonObject).content
    val key = (content["_id"] as JsonLiteral).content
    val value = content["value"].toString()

    ConsumerRecord<String, String>("topic", 0, 0L, key, value)
}.getOrThrow()

@ImplicitReflectionSerializer
fun main() {
    val sikkerhetstiltak = Json.nonstrict.parse<Person.Sikkerhetstiltak>(sikker)
    val bostedsadresse = Json.nonstrict.parse<Person.Bostedsadresse>(bosted)
    val testQuery = Json.nonstrict.parse<Query>(test)

    val m: MutableMap<String, SalesforceObjectBase> = mutableMapOf()

    val cRecords = File("./src/test/resources/compassExport.json").readLines().map { line ->
        val jsonElement = json.parseJson(line)
        jsonElement.getTopicValuePair()
    }

        cRecords.forEach { cr ->

            when (val v = cr.value()) {
                null -> m[cr.key()] = NoSalesforceObject
                is String -> if (v.isNotEmpty()) {
                    when (val query = v.getQueryFromJson()) {
                        is InvalidQuery -> Unit
                        is Query -> {
                            // if (query.isAlive && query.inRegion("54"))
                            log.info { "Topic value - $v" }
                            log.info { "Valid Query Object - $query" }
                            val pm = query.createPersonCMessage()
                            val am = query.createAccountMessage()
                            if (pm is PersonCMessage && am is AccountMessage) {
                                m[cr.key()] =
                                        SalesforceObject(
                                                personCObject = pm,
                                                accountObject = am
                                        )
                            } else {
                                log.error { "Fail createing PeronC or Account message from Query." }
                            }
                        }
                    }
                }
            }
        }
    println("Size - ${m.count()}")
    }

val bosted = """{"angittFlyttedato":"2019-09-05","coAdressenavn":"","vegadresse":null,"matrikkeladresse":{"matrikkelId":null,"tilleggsnavn":null,"postnummer":"0561","kommunenummer":"0301","koordinater":null},"ukjentBosted":null,"folkeregistermetadata":{"ajourholdstidspunkt":"2019-09-10T10:54:29","gyldighetstidspunkt":"2019-09-05T00:00:00","opphoerstidspunkt":null,"kilde":"","aarsak":"Annen tilgang","sekvens":null},"metadata":{"opplysningsId":"4070c8ce-0679-47d1-8cde-46c7a4b16320","master":"Freg","endringer":[{"type":"OPPRETT","registrert":"2020-03-03T09:52:17.104","registrertAv":"Folkeregisteret","systemkilde":"FREG","kilde":"FREG"}]}}"""
val sikker = """{"tiltakstype":"TOAN","beskrivelse":"To ansatte i samtale","kontaktperson":{"personident":"Z992630","enhet":"0312"},"gyldigFraOgMed":"2020-02-25","gyldigTilOgMed":"2020-03-02","metadata":{"opplysningsId":"025323b7-734c-4268-b25f-3352047201ee","master":"PDL","endringer":[{"type":"OPPRETT","registrert":"2020-02-25T13:40:30.09","registrertAv":"z992630","systemkilde":"srvgosys","kilde":"Gosys"}]}}"""
val test = """{"hentPerson":{"adressebeskyttelse":[{"gradering":"UGRADERT","folkeregistermetadata":{"ajourholdstidspunkt":"2020-03-03T08:15:45.563","gyldighetstidspunkt":"2020-03-03T08:15:45.563","opphoerstidspunkt":null,"kilde":"Dolly","aarsak":null,"sekvens":null},"metadata":{"opplysningsId":"9c62466a-5713-4e6a-b4a7-eb2e1ac305ec","master":"FREG","endringer":[{"type":"OPPRETT","registrert":"2020-03-03T08:15:45.491","registrertAv":"Folkeregisteret","systemkilde":"FREG","kilde":"Dolly"}]}}],"bostedsadresse":[],"deltBosted":[],"doedsfall":[],"falskIdentitet":null,"familierelasjoner":[],"foedsel":[{"foedselsaar":null,"foedselsdato":"1981-10-16","foedeland":null,"foedested":null,"foedekommune":null,"metadata":{"opplysningsId":"acbb6dcc-325a-46bb-8e25-a0582c3f2abd","master":"FREG","endringer":[{"type":"OPPRETT","registrert":"2020-03-03T08:15:45.365","registrertAv":"Folkeregisteret","systemkilde":"FREG","kilde":"Dolly"}]}}],"folkeregisteridentifikator":[{"identifikasjonsnummer":"16108122874","type":"FNR","status":"I_BRUK","folkeregistermetadata":{"ajourholdstidspunkt":"2020-03-03T08:15:45.362","gyldighetstidspunkt":"2020-03-03T08:15:45.362","opphoerstidspunkt":null,"kilde":"srvdolly","aarsak":null,"sekvens":null},"metadata":{"opplysningsId":"a8b57b10-1c36-4014-b7fa-aafd9d4fb257","master":"Freg","endringer":[{"type":"OPPRETT","registrert":"2020-03-03T08:15:45.447","registrertAv":"Folkeregisteret","systemkilde":"FREG","kilde":"srvdolly"}]}}],"folkeregisterpersonstatus":[],"fullmakt":[],"identitetsgrunnlag":[],"kontaktinformasjonForDoedsbo":[],"kjoenn":[{"kjoenn":"KVINNE","folkeregistermetadata":{"ajourholdstidspunkt":"2020-03-03T08:15:45.563","gyldighetstidspunkt":"2020-03-03T08:15:45.563","opphoerstidspunkt":null,"kilde":"Dolly","aarsak":null,"sekvens":null},"metadata":{"opplysningsId":"c91429fe-6578-4331-b678-77c5eda82453","master":"FREG","endringer":[{"type":"OPPRETT","registrert":"2020-03-03T08:15:45.471","registrertAv":"Folkeregisteret","systemkilde":"FREG","kilde":"Dolly"}]}}],"navn":[{"fornavn":"STERK","mellomnavn":null,"etternavn":"RISPBÆRBUSK","forkortetNavn":"RISPBÆRBUSK STERK","originaltNavn":null,"folkeregistermetadata":{"ajourholdstidspunkt":"2020-03-03T08:15:45.469","gyldighetstidspunkt":"2020-03-03T08:15:45.469","opphoerstidspunkt":null,"kilde":"Dolly","aarsak":null,"sekvens":0},"metadata":{"opplysningsId":"919d1722-a408-4c53-ad97-d5b018443789","master":"FREG","endringer":[{"type":"OPPRETT","registrert":"2020-03-03T08:15:45.405","registrertAv":"Folkeregisteret","systemkilde":"FREG","kilde":"Dolly"}]}}],"opphold":[],"sikkerhetstiltak":[{"tiltakstype":"TOAN","beskrivelse":"To ansatte i samtale","kontaktperson":{"personident":"Z992630","enhet":"0312"},"gyldigFraOgMed":"2020-02-25","gyldigTilOgMed":"2020-03-02","metadata":{"opplysningsId":"025323b7-734c-4268-b25f-3352047201ee","master":"PDL","endringer":[{"type":"OPPRETT","registrert":"2020-02-25T13:40:30.09","registrertAv":"z992630","systemkilde":"srvgosys","kilde":"Gosys"}]}},{"tiltakstype":"TFUS","beskrivelse":"Telefonisk utestengelse","kontaktperson":{"personident":"A142467","enhet":"4402"},"gyldigFraOgMed":"2020-02-11","gyldigTilOgMed":"2020-02-27","metadata":{"opplysningsId":"d7b1e5b0-17c3-4593-922c-8df3edb07d08","master":"PDL","endringer":[{"type":"OPPRETT","registrert":"2020-02-11T16:49:37.386","registrertAv":"srvgosys","systemkilde":"Z991770","kilde":"Gosys"},{"type":"OPPHOER","registrert":"2020-02-14T10:40:05.241","registrertAv":"Z991770","systemkilde":"srvgosys","kilde":"Gosys"}]}},{"tiltakstype":"FYUS","beskrivelse":"Fysisk utestengelse","kontaktperson":null,"gyldigFraOgMed":"2019-11-11","gyldigTilOgMed":"2020-10-30","metadata":{"opplysningsId":"1b575e6d-bd79-40a0-8df2-0109c54ec86a","master":"PDL","endringer":[{"type":"OPPRETT","registrert":"2019-11-11T00:00:00","registrertAv":"ukjent","systemkilde":"srvgosys","kilde":"TPS"}]}}],"sivilstand":[],"statsborgerskap":[{"land":"NOR","gyldigFraOgMed":null,"gyldigTilOgMed":null,"folkeregistermetadata":{"ajourholdstidspunkt":"2020-03-03T08:15:45.563","gyldighetstidspunkt":"2020-03-03T08:15:45.563","opphoerstidspunkt":null,"kilde":"Dolly","aarsak":null,"sekvens":null},"metadata":{"opplysningsId":"51a6fbf5-9e8d-4938-b434-54c17e59ecd6","master":"FREG","endringer":[{"type":"OPPRETT","registrert":"2020-03-03T08:15:45.512","registrertAv":"Folkeregisteret","systemkilde":"FREG","kilde":"Dolly"}]}}],"tilrettelagtKommunikasjon":[],"utenlandskIdentifikasjonsnummer":[],"telefonnummer":[],"innflyttingTilNorge":[],"utflyttingFraNorge":[]},"hentIdenter":{"identer":[{"ident":"16108122874","historisk":false,"gruppe":"FOLKEREGISTERIDENT","metadata":null},{"ident":"2719195785297","historisk":false,"gruppe":"AKTORID","metadata":null}]}}"""
