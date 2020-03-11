package no.nav.pdlsf

import io.kotlintest.specs.StringSpec
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.UnstableDefault

@UseExperimental(UnstableDefault::class)
@ImplicitReflectionSerializer
class PDLTests : StringSpec({
//    when (val query = value.getQueryFromJson()) {
//        is InvalidQuery -> Unit
//        is Query -> // if (query.isAlive && query.inRegion("54"))
//            println(query)
//    }

//    val adressebeskyttelse: String = query?.hentPerson?.adressebeskyttelse?.findGjeldeneAdressebeskytelse() ?: ""
//
//    val sikkerhetstiltak: List<Person.Sikkerhetstiltak>? = query?.hentPerson?.sikkerhetstiltak?.findGjelendeSikkerhetstiltak()
//    println(sikkerhetstiltak)
//
//    val kam: KafkaAccountMessage? = query?.createKafkaAccountMessage() ?: null
//    val kpm: KafkaPersonCMessage? = query?.createKafkaPersonCMessage() ?: null
//    println(kam)
//    println(kpm)
})

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
