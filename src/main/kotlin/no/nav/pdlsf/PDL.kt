package no.nav.pdlsf

import kotlinx.serialization.ContextualSerialization
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.UnstableDefault
import kotlinx.serialization.json.Json
import kotlinx.serialization.parse
import mu.KotlinLogging
import org.joda.time.LocalDate
import org.joda.time.LocalDateTime
import org.joda.time.LocalDateTime.now

private val log = KotlinLogging.logger { }

@UnstableDefault
@ImplicitReflectionSerializer
fun main() {
    val query = value.getQueryFromJsonString()
    println(query)

    val adressebeskyttelse: String = query.hentPerson.adressebeskyttelse.findGjeldeneAdressebeskytelse()

    val sikkerhetstiltak: List<Person.Sikkerhetstiltak>? = query.hentPerson.sikkerhetstiltak.findGjelendeSikkerhetstiltak()
    println(sikkerhetstiltak)

    val kam: KafkaAccountMessage = query.createKafkaAccountMessage()
    val kpm: KafkaPersonCMessage = query.createKafkaPersonCMessage()
    println(kam)
    println(kpm)
}

private fun List<Person.Sikkerhetstiltak>.findGjelendeSikkerhetstiltak(): List<Person.Sikkerhetstiltak>? {
    return this.filter { it.gyldigTilOgMed.isAfter(LocalDate.now()) }
            .filter { it.metadata.endringer.filter { it.type.name.equals(Endringstype.OPPHOE.name) }.isEmpty() }
}

private fun List<Person.Adressebeskyttelse>.findGjeldeneAdressebeskytelse(): String {
    return this.sortedWith(nullsFirst(compareBy { it.folkeregistermetadata.gyldighetstidspunkt })).firstOrNull { isNotOpphoert(it.folkeregistermetadata) }?.gradering?.name.orEmpty()
}

// TODO:: Sjekke denne... Usikker på om denne konverteringen fra String til LocalDatetime er Ok, men det er en feil som kommer å går med @ContextualSerialization val opphoerstidspunkt: LocalDateTime?,
private fun isNotOpphoert(folkeregistermetadata: Folkeregistermetadata): Boolean {
    return now().isBefore(LocalDateTime(folkeregistermetadata.opphoerstidspunkt))
}

private fun List<Person.Navn>.findGjelendeFregNavn(): Person.Navn {
    return this.filter { (it.folkeregistermetadata != null) }.sortedBy { it.folkeregistermetadata?.sekvens }.first()
}

fun Query.createKafkaPersonCMessage(): KafkaPersonCMessage {
    return KafkaPersonCMessage(
            gradering = hentPerson.adressebeskyttelse.findGjeldeneAdressebeskytelse(),
            sikkerhetstiltak = hentPerson.sikkerhetstiltak.findGjelendeSikkerhetstiltak()?.toSfListString().orEmpty(),
            kommunenummer = hentPerson.bostedsadresse.findGjelendeBostedsadresse()?.matrikkeladresse?.kommunenummer.orEmpty() // TODO :: Ikke påkrevdfelt. Hvordan håndtere dette
    ) }

private fun List<Person.Sikkerhetstiltak>.toSfListString(): String {
    return this.map { it.beskrivelse }.toList().joinToString(";")
}

private fun List<Person.Bostedsadresse>.findGjelendeBostedsadresse(): Person.Bostedsadresse? {
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
        @ContextualSerialization val registrert: String,
        val registrertAv: String,
        val systemkilde: String,
        val kilde: String
    )
}
@Serializable
data class Folkeregistermetadata(
    @ContextualSerialization val ajourholdstidspunkt: String?,
    @ContextualSerialization val gyldighetstidspunkt: String?,
    @ContextualSerialization val opphoerstidspunkt: String?,
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
    val adressebeskyttelse: List<Adressebeskyttelse>,
    val bostedsadresse: List<Bostedsadresse>,
    val doedsfall: List<Doedsfall>,
    val sikkerhetstiltak: List<Sikkerhetstiltak>,
    val navn: List<Navn>

) {

    @Serializable
    data class Bostedsadresse(
        @ContextualSerialization val angittFlyttedato: LocalDate,
        val coAdressenavn: String,
        val vegadresse: Vegadresse,
        val matrikkeladresse: Matrikkeladresse,
        val ukjentBosted: UkjentBosted,
        val folkeregistermetadata: Folkeregistermetadata,
        val metadata: Metadata
    )

    @Serializable
    data class Doedsfall(
        @ContextualSerialization val doedsdato: LocalDate?,
        val metadata: Metadata
    )

    @Serializable
    data class Sikkerhetstiltak(
        val tiltakstype: String,
        val beskrivelse: String,
        val kontaktperson: SikkerhetstiltakKontaktperson,
        @ContextualSerialization val gyldigFraOgMed: LocalDate,
        @ContextualSerialization val gyldigTilOgMed: LocalDate,
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

val value: String = """
    {
      "hentPerson": {
        "adressebeskyttelse": [
          
        ],
        "bostedsadresse": [
          
        ],
        "deltBosted": [
          
        ],
        "doedsfall": [
          
        ],
        "falskIdentitet": null,
        "familierelasjoner": [
          
        ],
        "foedsel": [
          {
            "foedselsaar": 1949,
            "foedselsdato": "1949-11-10",
            "foedeland": "NOR",
            "foedested": null,
            "foedekommune": "1505",
            "metadata": {
              "opplysningsId": "e3f66ee8-8fa6-483f-97cb-b8793ebabe96",
              "master": "Freg",
              "endringer": [
                {
                  "type": "OPPRETT",
                  "registrert": "2019-09-06T04:24:01.516",
                  "registrertAv": "Folkeregisteret",
                  "systemkilde": "FREG",
                  "kilde": "KILDE_DSF"
                }
              ]
            }
          }
        ],
        "folkeregisteridentifikator": [
          {
            "identifikasjonsnummer": "10114900171",
            "type": "FNR",
            "status": "I_BRUK",
            "folkeregistermetadata": {
              "ajourholdstidspunkt": "2019-09-05T11:17:51.992",
              "gyldighetstidspunkt": null,
              "opphoerstidspunkt": null,
              "kilde": "KILDE_DSF",
              "aarsak": null,
              "sekvens": null
            },
            "metadata": {
              "opplysningsId": "4d664db0-1b42-4fc5-a95e-84ffa36f2bcf",
              "master": "Freg",
              "endringer": [
                {
                  "type": "OPPRETT",
                  "registrert": "2019-06-25T09:54:52.624",
                  "registrertAv": "Folkeregisteret",
                  "systemkilde": "FREG",
                  "kilde": "dsf-folkeregister-synkronisering"
                },
                {
                  "type": "KORRIGER",
                  "registrert": "2019-09-06T04:24:01.516",
                  "registrertAv": "Folkeregisteret",
                  "systemkilde": "FREG",
                  "kilde": "KILDE_DSF"
                }
              ]
            }
          }
        ],
        "folkeregisterpersonstatus": [
          {
            "status": "bosatt",
            "forenkletStatus": "bosattEtterFolkeregisterloven",
            "folkeregistermetadata": {
              "ajourholdstidspunkt": "2019-09-05T11:17:51.992",
              "gyldighetstidspunkt": "2019-09-05T11:17:51.992",
              "opphoerstidspunkt": null,
              "kilde": "KILDE_DSF",
              "aarsak": null,
              "sekvens": null
            },
            "metadata": {
              "opplysningsId": "260ca299-a7e4-4105-a81e-beda379f0fcd",
              "master": "Freg",
              "endringer": [
                {
                  "type": "OPPRETT",
                  "registrert": "2019-09-06T04:24:01.516",
                  "registrertAv": "Folkeregisteret",
                  "systemkilde": "FREG",
                  "kilde": "KILDE_DSF"
                }
              ]
            }
          }
        ],
        "fullmakt": [
          
        ],
        "identitetsgrunnlag": [
          
        ],
        "kontaktinformasjonForDoedsbo": [
          
        ],
        "kjoenn": [
          {
            "kjoenn": "MANN",
            "folkeregistermetadata": {
              "ajourholdstidspunkt": null,
              "gyldighetstidspunkt": null,
              "opphoerstidspunkt": null,
              "kilde": "KILDE_DSF",
              "aarsak": null,
              "sekvens": null
            },
            "metadata": {
              "opplysningsId": "7f6e41f4-527b-4ad7-bd4e-d2db848ee815",
              "master": "Freg",
              "endringer": [
                {
                  "type": "OPPRETT",
                  "registrert": "2019-10-10T09:44:45.637",
                  "registrertAv": "Folkeregisteret",
                  "systemkilde": "FREG",
                  "kilde": "KILDE_DSF"
                }
              ]
            }
          }
        ],
        "navn": [
          {
            "fornavn": "VIKTOR",
            "mellomnavn": null,
            "etternavn": "ROSENBERG",
            "forkortetNavn": "VIKTOR  ROSENBERG",
            "originaltNavn": null,
            "folkeregistermetadata": {
              "ajourholdstidspunkt": null,
              "gyldighetstidspunkt": "2019-01-01T00:00:00",
              "opphoerstidspunkt": null,
              "kilde": "KILDE_DSF",
              "aarsak": "Første gangs navnevalg",
              "sekvens": 0
            },
            "metadata": {
              "opplysningsId": "906c8218-9956-4ec8-91a4-aa9f9790df64",
              "master": "Freg",
              "endringer": [
                {
                  "type": "OPPRETT",
                  "registrert": "2019-10-11T14:25:39.382",
                  "registrertAv": "Folkeregisteret",
                  "systemkilde": "FREG",
                  "kilde": "KILDE_DSF"
                }
              ]
            }
          }
        ],
        "opphold": [
          
        ],
        "sikkerhetstiltak": [
          
        ],
        "sivilstand": [
          
        ],
        "statsborgerskap": [
          
        ],
        "tilrettelagtKommunikasjon": [
          
        ],
        "utenlandskIdentifikasjonsnummer": [
          
        ],
        "telefonnummer": [
          
        ]
      },
      "hentIdenter": {
        "identer": [
          {
            "ident": "10114900171",
            "historisk": false,
            "gruppe": "FOLKEREGISTERIDENT"
          },
          {
            "ident": "2733733247036",
            "historisk": false,
            "gruppe": "AKTORID"
          }
        ]
      }
    }
""".trimIndent()
