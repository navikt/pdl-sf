package no.nav.pdlsf

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.UnstableDefault
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig

private val log = KotlinLogging.logger {}

@UseExperimental(UnstableDefault::class)
@ImplicitReflectionSerializer
internal fun work(params: Params) {

    log.info { "bootstrap work session starting" }
    val m: MutableMap<String, SalesforceObjectBase> = mutableMapOf()

    getKafkaConsumerByConfig<String, String>(
        mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to params.kafkaBrokers,
            "schema.registry.url" to params.kafkaSchemaRegistry,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java,
            ConsumerConfig.GROUP_ID_CONFIG to params.kafkaClientID,
            ConsumerConfig.CLIENT_ID_CONFIG to params.kafkaClientID,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            // ConsumerConfig.MAX_POLL_RECORDS_CONFIG to 100, // 200 is the maximum batch size accepted by salesforce
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false"
        ).let { cMap ->
            if (params.kafkaSecurityEnabled())
                cMap.addKafkaSecurity(params.kafkaUser, params.kafkaPassword, params.kafkaSecProt, params.kafkaSaslMec)
            else cMap
        },
        listOf(params.kafkaTopic), fromBeginning = true
    ) { cRecords ->
        if (!cRecords.isEmpty) {
            log.info { "Number of records : ${cRecords.count()}" }

            cRecords.forEach { cr ->

                when (val v = cr.value()) {
                    null -> m[cr.key()] = NoSalesforceObject
                    is String -> if (v.isNotEmpty())
                        when (val query = v.getQueryFromJson()) {
                            is InvalidQuery -> Unit
                            is Query -> // if (query.isAlive && query.inRegion("54"))
                                m[cr.key()] =
                                    SalesforceObject(
                                            personCObject = query.createKafkaPersonCMessage(),
                                            accountObject = query.createKafkaAccountMessage()
                                    ).also { log.info { "Valid Query Object - $query" } }
                        }
                }
            }

            ConsumerStates.IsFinished
        } else {
            log.info { "Kafka events completed for now - leaving kafka consumer loop" }
            ConsumerStates.IsFinished
        }
    }

    log.info { "Ready to prepare max ${m.size} records for salesforce" }

    m.filterValues { it is SalesforceObject }
        .map { it.value }
        .toList()
        .filterIsInstance<SalesforceObject>()
        .let { l ->

            if (l.isNotEmpty()) {
                log.info { "Processing list of ${l.size} Salesforce entries" }

                val toAccountCSV = l.map { it.accountObject }.toAccountCSV()
                val toPersonCCSV = l.map { it.personCObject }.toPersonCCSV()

                doAuthorization { authorization ->
                    authorization.createJob(
                            JobSpecification(
                                    obj = "AccountT_c__c", // TODO :: Endre til Account, men opprette custom og teste mot først
                                    operation = Operation.INSERT
                            )
                    ) { completeAccountCSVBatch ->
                        completeAccountCSVBatch(toAccountCSV)
                    }
                    authorization.createJob(
                            JobSpecification(
                                    obj = "PersonT_c__c", // TODO :: Endre til Person__c, men opprette custom og teste mot først
                                    operation = Operation.INSERT
                            )
                    ) { completePersonCCSVBatch ->
                        completePersonCCSVBatch(toPersonCCSV)
                    }
                }
            }
        }
}
