package novel_lab.f01_source_operator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FromKafka {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String brokers  = "hadoop101:9092,hadoop102:9092,hadoop103:9092";
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics("topic_book_info")
                .setGroupId("flink_consumer")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStreamSource<String> kafka_source = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
//        kafka_source.print();
        kafka_source.map(data -> {
            String[] arr = data.split(",");
            String bid = arr[0];
            String title = arr[1];
            String writer = arr[2];
            String type  = arr[3];
            String gender  = arr[4];
            Tuple4 tuple4 = new Tuple4(bid,title,writer,gender);
            return tuple4;
        })
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING))
                .print();


        env.execute();

    }
}
