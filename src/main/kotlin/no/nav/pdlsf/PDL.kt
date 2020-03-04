package no.nav.pdlsf

import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.parse
import mu.KotlinLogging

private val log = KotlinLogging.logger { }

@ImplicitReflectionSerializer

fun Query.createKafkaPersonCMessage(): KafkaPersonCMessage {
    // TODO :: Filter så vi har indeksen på gjeldene navn da historikk er med
    return KafkaPersonCMessage(
            gradering = hentPerson.adressebeskyttelse?.first()?.gradering?.name ?: "",
            sikkerhetstiltak = hentPerson.sikkerhetstiltak?.first()?.beskrivelse ?: "",
            kommunenummer = "000000" // TODO :: Ikke implementert i PDL skal Bostedsadresse i helga
    ) }

fun Query.createKafkaAccountMessage(): KafkaAccountMessage {
    // TODO :: Filter så vi har indeksen på gjeldene navn da historikk er med
    return KafkaAccountMessage(
            fnr = this.hentIdenter.identer.filter { !it.historisk && it.gruppe.name.equals(IdentGruppe.FOLKEREGISTERIDENT) }.first().ident,
            fornavn = this.hentPerson.navn[0].fornavn,
            mellomnavn = this.hentPerson.navn[0].mellomnavn.orEmpty(),
            etternavn = this.hentPerson.navn[0].etternavn
    )
}

@ImplicitReflectionSerializer
fun String.getQueryFromJsonString() = runCatching { Json.nonstrict.parse<Query>(this) }.getOrThrow()

@Serializable
enum class IdentGruppe {
    AKTORID,
    FOLKEREGISTERIDENT,
    NPID
}
@Serializable
enum class AdressebeskyttelseGradering {
    STRENGT_FORTROLIG_UTLAND,
    STRENGT_FORTROLIG,
    FORTROLIG,
    UGRADERT
}

@Serializable
enum class Endringstype {
    OPPRETT,
    KORRIGER,
    OPPHOE,
    ANNULER
}

@Serializable
data class Metadata(
    val opplysningsId: String,
    val master: String,
    val endringer: List<Endring>
) {
    @Serializable
    data class Endring(
        val type: Endringstype,
        val registrert: String, // TODO:: For now DateTime is treated as string
        val registrertAv: String,
        val systemkilde: String,
        val kilde: String
    )
}
@Serializable
data class Folkeregistermetadata(
    val ajourholdstidspunkt: String?, // TODO:: For now DateTime is treated as string
    val gyldighetstidspunkt: String?, // TODO:: For now DateTime is treated as string
    val opphoerstidspunkt: String?, // TODO:: For now DateTime is treated as string
    val kilde: String?,
    val aarsak: String?,
    val sekvens: Int?
)
@Serializable
data class Query(
    val hentPerson: Person,
    val hentIdenter: Identliste
)
@Serializable
data class Identliste(
    val identer: List<IdentInformasjon>
) {
    @Serializable
    data class IdentInformasjon(
        val ident: String,
        val historisk: Boolean,
        val gruppe: IdentGruppe
    )
}
@Serializable
data class Person(
    val adressebeskyttelse: List<Adressebeskyttelse>?,
    val sikkerhetstiltak: List<Sikkerhetstiltak>?,
    val navn: List<Navn>

) {

    @Serializable
    data class Sikkerhetstiltak(
        val tiltakstype: String,
        val beskrivelse: String,
        val kontaktperson: SikkerhetstiltakKontaktperson,
        val gyldigFraOgMed: String, // TODO:: For now Date is treated as string
        val gyldigTilOgMed: String, // TODO:: For now Date is treated as string
        val metadata: Metadata
    ) {
        @Serializable
        data class SikkerhetstiltakKontaktperson(
            val personident: String,
            val enhet: String
        )
    }

    @Serializable
    data class Navn(
        val fornavn: String,
        val mellomnavn: String?,
        val etternavn: String,
        val forkortetNavn: String?,
        val originaltNavn: String?,
        val folkeregistermetadata: Folkeregistermetadata?,
        val metadata: Metadata
    )
    @Serializable
    data class Adressebeskyttelse(
        val gradering: AdressebeskyttelseGradering
    )
}

data class KafkaAccountMessage(
    val fnr: String,
    val fornavn: String,
    val mellomnavn: String,
    val etternavn: String
) {
    fun toCSVLine() = """"$fnr","$fornavn","$mellomnavn","$etternavn""""
}

fun List<KafkaAccountMessage>.toAccountCSV(): String = StringBuilder().let { sb ->
    sb.appendln("INT_PersonIdent__c,FirstName,MiddleName,LastName")
    this.forEach {
        sb.appendln(it.toCSVLine())
    }
    sb.toString()
}

data class KafkaPersonCMessage(
    val gradering: String,
    val sikkerhetstiltak: String,
    val kommunenummer: String
) {
    fun toCSVLine() = """"$gradering","$sikkerhetstiltak","$kommunenummer","${kommunenummer.substring(0,1)}"""
}

fun List<KafkaPersonCMessage>.toPersonCCSV(): String = StringBuilder().let { sb ->
    sb.appendln("INT_Confidential__c,INT_SecurityMeasures__c,INT_MunichipalityNumber__c,INT_RegionNumber__c")
    this.forEach {
        sb.appendln(it.toCSVLine())
    }
    sb.toString()
}

data class SalsforceObject(
    val personCObject: KafkaPersonCMessage,
    val accountObject: KafkaAccountMessage
)