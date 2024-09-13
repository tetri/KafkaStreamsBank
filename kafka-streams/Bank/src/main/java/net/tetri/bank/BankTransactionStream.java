package net.tetri.bank;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

public class BankTransactionStream {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-transaction-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        // Definindo o KStream com chave String e valor Double
        KStream<String, Double> transactions = builder
                .stream("transactions-topic", Consumed.with(Serdes.String(), Serdes.Double()));

        // Agrupando por chave (userId)
        KGroupedStream<String, Double> groupedByKey = transactions.groupByKey();

        // Agregação: saldo por cliente
        KTable<String, Double> accountBalances = groupedByKey
                .reduce((aggValue, newValue) -> aggValue + newValue);

        // Enviar o saldo atualizado para o tópico de saída
        accountBalances.toStream().to("account-balances-topic", Produced.with(Serdes.String(), Serdes.Double()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
