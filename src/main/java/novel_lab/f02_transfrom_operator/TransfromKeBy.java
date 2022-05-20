package novel_lab.f02_transfrom_operator;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransfromKeBy {
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
            String gender  = arr[4];
            Tuple5 tuple5 = new Tuple5(bid,title,writer,gender,1);
            return tuple5;
        })
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.INT))
                .keyBy(tuple5 -> tuple5.f3)
                //输入元组的索引,提取数据作为加法的参数
                .sum(4)
                .print();



        env.execute();

    }
}
