package no.nav.pdlsf

import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.parse
import mu.KotlinLogging

private val log = KotlinLogging.logger { }

@ImplicitReflectionSerializer
fun main() {
/*
    val kafkaAccountMessage = createKafkaAccountMessage(value)
    println(kafkaAccountMessage)
    val kafkaPersonCMessage = createKafkaPersonCMessage(value)
    println(kafkaPersonCMessage)
*/
    val query = value.getQueryFromJsonString()
    println(query)
    val kam: KafkaAccountMessage = query.createKafkaAccountMessage()
    val kpm: KafkaPersonCMessage = query.createKafkaPersonCMessage()
    println(kam)
    println(kpm)
}

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

/*

fun createKafkaPersonCMessage(jsonString: String): KafkaPersonCMessage {
    val jsonElement = kotlinx.serialization.json.Json.nonstrict.parseJson(jsonString)


    val joHentPerson = jsonElement.jsonObject["hentPerson"]?.jsonObject ?: throw error("Unable to retrieve JsonObject hentPerson from json string")
    // TODO:: Her må vi se på  hvordan vi håndterer dette da vi ikke vil forholde oss til hostoriske elemnter i denne arrayen
    val gradering = joHentPerson["adressebeskyttelse"]?.jsonArray?.content?.getOrNull(0)?.jsonObject?.get("gradering")?.contentOrNull  // throw error("Unable to retrieve JsonObject hentPerson.navn from json string")
    val sikkerhetstiltak = joHentPerson["sikkerhetstiltak"]?.jsonArray?.content?.getOrNull(0)?.jsonObject?.get("beskrivelse")?.contentOrNull   // throw error("Unable to retrieve JsonObject hentPerson.sikkerhetstiltak from json string")
    return KafkaPersonCMessage(
            gradering = gradering.orEmpty(),
            sikkerhetstiltak = sikkerhetstiltak.orEmpty(),
            kommunenummer = "000000" //TODO :: Se på bosted
    )
}


fun createKafkaAccountMessage(jsonString: String): KafkaAccountMessage {
    val jsonElement = kotlinx.serialization.json.Json.nonstrict.parseJson(jsonString)


    val joHentPerson = jsonElement.jsonObject["hentPerson"]?.jsonObject ?: throw error("Unable to retrieve JsonObject hentPerson from json string")
    //TODO:: Her må vi se på  hvordan vi håndterer dette da vi ikke vil forholde oss til hostoriske elemnter i denne arrayen
    val joNavn = joHentPerson["navn"]?.jsonArray!!.content[0].jsonObject ?: throw error("Unable to retrieve JsonObject hentPerson.navn from json string")
    val fornavn: String = joNavn["fornavn"]!!.content
    val mellomnavn: String? = joNavn["mellomnavn"]?.contentOrNull
    val etternavn: String = joNavn["etternavn"]!!.content

    val joHentIdenter = jsonElement.jsonObject["hentIdenter"]?.jsonObject
            ?: throw error("Unable to retrieve JsonObject hentIdenter from json string")
    val fnr = joHentIdenter["identer"]?.jsonArray?.content?.filter { (it.jsonObject["gruppe"]?.content == "FOLKEREGISTERIDENT") && (it.jsonObject["historisk"]?.content == "false") }?.first()!!.jsonObject["ident"]?.content
            ?: throw error("Unable to retrieve FOLKEREGISTERIDENT from that are active from hentIndent.ident in json string\"")

    return KafkaAccountMessage(
            fnr = fnr,
            fornavn = fornavn,
            mellomnavn = mellomnavn.orEmpty(),
            etternavn = etternavn
    )


}
*/

val value: String = """
    {
      "hentPerson": {
        "adressebeskyttelse": [
          
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
