package novel_lab.f00_pojo;

import java.sql.Timestamp;

public class IdfCount {
    public String user_id;
    public int count;
    public long windowStartTime;
    public long windowEndTime;

    public IdfCount() {
    }

    public IdfCount(String user_id,int count,long start,long end) {
        this.user_id = user_id;
        this.count = count;
        this.windowStartTime = start;
        this.windowEndTime = end;
    }


    @Override
    public String toString() {
        return (user_id + "," + count + "," + new Timestamp(windowStartTime) + "," + new Timestamp(windowEndTime));
    }
}
