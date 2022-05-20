package novel_lab.f01_source_operator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FromProg  {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.addSource(new ProgSource()).print();
        env.execute();

    }
}
