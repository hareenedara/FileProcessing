/**
 * Created by edara on 8/22/17.
 */
public class KafkaSendException extends Exception {
    private String msg;

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
