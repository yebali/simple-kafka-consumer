import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.Properties

class SimpleConsumer {
    companion object {
        const val TOPIC_NAME = "test"
        const val BOOTSTRAP_SERVERS = "127.0.0.1:9092"
        const val GROUP_ID = "test-group"
    }
}
fun main(args: Array<String>) {
    val configs = Properties()
    val logger = LoggerFactory.getLogger(SimpleConsumer.javaClass)

    configs[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = SimpleConsumer.BOOTSTRAP_SERVERS
    configs[ConsumerConfig.GROUP_ID_CONFIG] = SimpleConsumer.GROUP_ID
    configs[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
    configs[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name

    val consumer = KafkaConsumer<String, String>(configs)
    consumer.subscribe(listOf(SimpleConsumer.TOPIC_NAME))

    while (true) {
        val records = consumer.poll(Duration.ofSeconds(1)) // 카프카 브로커에 있는 데이터를 가져온다.
        records.map {
            logger.info("{}", it)
        }
    }
}
