package wordcount;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
public class WordCountOne {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> lineSource = env.readTextFile("input/words.txt");
        //编译期间存在泛型擦除的问题.
        FlatMapOperator<String, Tuple2<String, Long>> flatMapOperator = lineSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            //输入string
            String[] words = line.split(" ");
            for (String word : words) {
                //输出tuple
                out.collect(Tuple2.of(word, 1L));
            }
            //补充collector 返回结果的类型
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));
        AggregateOperator<Tuple2<String, Long>> sum = flatMapOperator.groupBy(0).sum(1);
        sum.print();

    }
}
