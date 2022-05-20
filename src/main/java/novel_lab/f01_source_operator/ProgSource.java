package novel_lab.f01_source_operator;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

public class ProgSource implements SourceFunction<String> {
    private String filePath = null;
    private int timeMills = 200;
    public ProgSource(){
    }
    public ProgSource(String filePath){
        this.filePath = filePath;
    }
    public ProgSource(int timeMills, String filePath){
        this.timeMills = timeMills;
        this.filePath = filePath;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        File f = new File(this.filePath);
        InputStreamReader read = new InputStreamReader(new FileInputStream(f),"UTF-8");
        BufferedReader reader=new BufferedReader(read);
        String temp = null;
        while((temp=reader.readLine())!=null &&!"".equals(temp)){
            ctx.collect(temp);
            Thread.sleep(timeMills);
        }
    }
    @Override
    public void cancel() {

    }
}