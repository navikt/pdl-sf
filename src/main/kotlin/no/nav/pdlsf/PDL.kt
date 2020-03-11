package no.nav.pdlsf

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalDateTime.now
import java.time.format.DateTimeFormatter
import kotlinx.serialization.Decoder
import kotlinx.serialization.Encoder
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.Serializable
import kotlinx.serialization.UnstableDefault
import kotlinx.serialization.internal.StringDescriptor
import kotlinx.serialization.json.Json
import kotlinx.serialization.parse
import kotlinx.serialization.withName
import mu.KotlinLogging

private val log = KotlinLogging.logger { }

// @Serializer(forClass = LocalDate::class)
object IsoLocalDateSerializer : LocalDateSerializer(DateTimeFormatter.ISO_DATE)

open class LocalDateSerializer(private val formatter: DateTimeFormatter) : KSerializer<LocalDate> {
    override val descriptor: SerialDescriptor = StringDescriptor.withName("java.time.LocalDate")
    override fun deserialize(decoder: Decoder): LocalDate {
        return LocalDate.parse(decoder.decodeString(), formatter)
    }

    override fun serialize(encoder: Encoder, obj: LocalDate) {
        encoder.encodeString(obj.format(formatter))
    }
}

// @Serializer(forClass = LocalDateTime::class)
object IsoLocalDateTimeSerializer : LocalDateTimeSerializer(DateTimeFormatter.ISO_DATE)

open class LocalDateTimeSerializer(private val formatter: DateTimeFormatter) : KSerializer<LocalDateTime> {
    override val descriptor: SerialDescriptor = StringDescriptor.withName("java.time.LocalDateTime")
    override fun deserialize(decoder: Decoder): LocalDateTime {
        return LocalDateTime.parse(decoder.decodeString(), formatter)
    }

    override fun serialize(encoder: Encoder, obj: LocalDateTime) {
        encoder.encodeString(obj.format(formatter))
    }
}

internal fun List<Person.Sikkerhetstiltak>.findGjelendeSikkerhetstiltak(): List<Person.Sikkerhetstiltak>? {
    return this.filter { it.gyldigTilOgMed.isAfter(LocalDate.now()) }
            .filter { it.metadata.endringer.filter { it.type.name.equals(Endringstype.OPPHOE.name) }.isEmpty() }
}

internal fun List<Person.Adressebeskyttelse>.findGjeldeneAdressebeskytelse(): String {
    return this.sortedWith(nullsFirst(compareBy { it.folkeregistermetadata.gyldighetstidspunkt })).firstOrNull { isNotOpphoert(it.folkeregistermetadata) }?.gradering?.name.orEmpty()
}

// TODO:: Sjekke denne... Usikker på om denne konverteringen fra String til LocalDatetime er Ok, men det er en feil som kommer å går med @ContextualSerialization val opphoerstidspunkt: LocalDateTime?,
private fun isNotOpphoert(folkeregistermetadata: Folkeregistermetadata): Boolean {
    return now().isBefore(folkeregistermetadata.opphoerstidspunkt)
}

internal fun List<Person.Navn>.findGjelendeFregNavn(): Person.Navn {
    return this.filter { (it.folkeregistermetadata != null) }.sortedBy { it.folkeregistermetadata?.sekvens }.first()
}

fun Query.createKafkaPersonCMessage(): KafkaPersonCMessage {
    return KafkaPersonCMessage(
            gradering = hentPerson.adressebeskyttelse.findGjeldeneAdressebeskytelse(),
            sikkerhetstiltak = hentPerson.sikkerhetstiltak.findGjelendeSikkerhetstiltak()?.toSfListString().orEmpty(),
            kommunenummer = hentPerson.bostedsadresse.findGjelendeBostedsadresse()?.matrikkeladresse?.kommunenummer.orEmpty() // TODO :: Ikke påkrevdfelt. Hvordan håndtere dette
    ) }

internal fun List<Person.Sikkerhetstiltak>.toSfListString(): String {
    return this.map { it.beskrivelse }.toList().joinToString(";")
}

internal fun List<Person.Bostedsadresse>.findGjelendeBostedsadresse(): Person.Bostedsadresse? {
    return this.sortedWith(nullsFirst(compareBy { it.folkeregistermetadata.gyldighetstidspunkt })).firstOrNull { isNotOpphoert(it.folkeregistermetadata) }
}

fun Query.createKafkaAccountMessage(): KafkaAccountMessage {
    return KafkaAccountMessage(
            fnr = this.hentIdenter.identer.first { !it.historisk && it.gruppe.name.equals(IdentGruppe.FOLKEREGISTERIDENT.name) }.ident,
            fornavn = this.hentPerson.navn.findGjelendeFregNavn().fornavn,
            mellomnavn = this.hentPerson.navn.findGjelendeFregNavn().mellomnavn.orEmpty(),
            etternavn = this.hentPerson.navn.findGjelendeFregNavn().etternavn
    )
}

@UnstableDefault
@ImplicitReflectionSerializer
fun String.getQueryFromJson() = runCatching {
    Json.nonstrict.parse<Query>(this)
    Metrics.sucessfulValueToQuery.inc()
}
        .onFailure {
            Metrics.invalidQuery.inc()
            log.error { "Cannot convert kafka value to query - ${it.localizedMessage}" }
        }
        .getOrDefault(InvalidQuery)

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
data class Vegadresse(
    val matrikkelId: Int?,
    val husnummer: String?,
    val husbokstav: String?,
    val bruksenhetsnummer: String?,
    val adressenavn: String?,
    val kommunenummer: String?,
    val tilleggsnavn: String?,
    val postnummer: String?,
    val koordinater: Koordinater
)

@Serializable
data class Matrikkeladresse(
    val matrikkelId: Int?,
    val bruksenhetsnummer: String?,
    val tilleggsnavn: String?,
    val postnummer: String?,
    val kommunenummer: String?,
    val koordinater: Koordinater?
)

@Serializable
data class UkjentBosted(
    val bostedskommune: String
)

@Serializable
data class Koordinater(
    val x: Float,
    val y: Float,
    val z: Float,
    val kvalitet: Int
)

@Serializable
data class Metadata(
    val opplysningsId: String,
    val master: String,
    val endringer: List<Endring>
) {
    @Serializable
    data class Endring(
        val type: Endringstype,
        @Serializable(with = IsoLocalDateTimeSerializer::class)
        val registrert: LocalDateTime,
        val registrertAv: String,
        val systemkilde: String,
        val kilde: String
    )
}
@Serializable
data class Folkeregistermetadata(
    @Serializable(with = IsoLocalDateTimeSerializer::class)
    val ajourholdstidspunkt: LocalDateTime?,
    @Serializable(with = IsoLocalDateTimeSerializer::class)
    val gyldighetstidspunkt: LocalDateTime?,
    @Serializable(with = IsoLocalDateTimeSerializer::class)
    val opphoerstidspunkt: LocalDateTime?,
    val kilde: String?,
    val aarsak: String?,
    val sekvens: Int?
)

sealed class QueryBase
object InvalidQuery : QueryBase()

@Serializable
data class Query(
    val hentPerson: Person,
    val hentIdenter: Identliste
) : QueryBase() {
    fun inRegion(r: String) = this.hentPerson.bostedsadresse.findGjelendeBostedsadresse()?.matrikkeladresse?.kommunenummer?.startsWith(r) ?: false
}

val Query.isAlive: Boolean
    get() = this.hentPerson.doedsfall.isNullOrEmpty()

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
    val adressebeskyttelse: List<Adressebeskyttelse>,
    val bostedsadresse: List<Bostedsadresse>,
    val doedsfall: List<Doedsfall>,
    val sikkerhetstiltak: List<Sikkerhetstiltak>,
    val navn: List<Navn>

) {

    @Serializable
    data class Bostedsadresse(
        @Serializable(with = IsoLocalDateSerializer::class)
        val angittFlyttedato: LocalDate,
        val coAdressenavn: String,
        val vegadresse: Vegadresse,
        val matrikkeladresse: Matrikkeladresse,
        val ukjentBosted: UkjentBosted,
        val folkeregistermetadata: Folkeregistermetadata,
        val metadata: Metadata
    )

    @Serializable
    data class Doedsfall(
        @Serializable(with = IsoLocalDateSerializer::class)
        val doedsdato: LocalDate?,
        val metadata: Metadata
    )

    @Serializable
    data class Sikkerhetstiltak(
        val tiltakstype: String,
        val beskrivelse: String,
        val kontaktperson: SikkerhetstiltakKontaktperson,
        @Serializable(with = IsoLocalDateSerializer::class)
        val gyldigFraOgMed: LocalDate,
        @Serializable(with = IsoLocalDateSerializer::class)
        val gyldigTilOgMed: LocalDate,
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
        val gradering: AdressebeskyttelseGradering,
        val folkeregistermetadata: Folkeregistermetadata,
        val metadata: Metadata
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
    sb.appendln("INT_PersonIdent_c__c,FirstName__c,MiddleName__c,LastName__c")
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
    fun toCSVLine() = """"$gradering","$sikkerhetstiltak","$kommunenummer","${kommunenummer.substring(0,1)}""""
}

fun List<KafkaPersonCMessage>.toPersonCCSV(): String = StringBuilder().let { sb ->
    sb.appendln("INT_Confidential_c__c,INT_SecurityMeasures_c__c,INT_MunichipalityNumber_c__c,INT_RegionNumber_c__c")
    this.forEach {
        sb.appendln(it.toCSVLine())
    }
    sb.toString()
}
sealed class SalesforceObjectBase

object NoSalesforceObject : SalesforceObjectBase()

data class SalesforceObject(
    val personCObject: KafkaPersonCMessage,
    val accountObject: KafkaAccountMessage
) : SalesforceObjectBase()
