package no.nav.pdlsf

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import mu.KotlinLogging
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig

private val log = KotlinLogging.logger {}

internal fun work(params: Params) {

    log.info { "bootstrap work session starting" }

        getKafkaConsumerByConfig<GenericRecord, GenericRecord>(
            mapOf(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to params.kafkaBrokers,
                "schema.registry.url" to params.kafkaSchemaRegistry,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java,
                ConsumerConfig.GROUP_ID_CONFIG to params.kafkaClientID,
                ConsumerConfig.CLIENT_ID_CONFIG to params.kafkaClientID,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG to 100, // 200 is the maximum batch size accepted by salesforce
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false"
            ).let { cMap ->
                if (params.kafkaSecurityEnabled())
                    cMap.addKafkaSecurity(params.kafkaUser, params.kafkaPassword, params.kafkaSecProt, params.kafkaSaslMec)
                else cMap
            },
            listOf(params.kafkaTopic), fromBeginning = true
        ) { cRecords ->
            if (!cRecords.isEmpty) {
                log.info { "Key: ${cRecords.first() .key()}" }
                log.info { "Value: ${cRecords.first() .value()}" }
                ConsumerStates.IsFinished
            } else {
                log.info { "Kafka events completed for now - leaving kafka consumer loop" }
                ConsumerStates.IsFinished
            }
        }
}
