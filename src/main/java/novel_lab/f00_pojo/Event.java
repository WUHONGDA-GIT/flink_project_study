package novel_lab.f00_pojo;

import java.sql.Timestamp;

public class Event {
    public  String user_id;
    public String action;
    public Long timeStamp;

    public Event() {
        super();
    }

    public Event(String user_id,String action,Long timeStamp) {
        this.user_id = user_id;
        this.action = action;
        this.timeStamp = timeStamp;
    }


    @Override
    public String toString() {
        return (user_id + "," + action + "," + new Timestamp(timeStamp));
    }
}
