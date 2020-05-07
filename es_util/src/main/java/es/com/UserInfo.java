package es.com;

import sun.rmi.runtime.Log;

import java.util.Date;


//@Data
public class UserInfo {

    private String userName;
    private Long sendDate;
    private String msg;
    private int id;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }
    // @Field(type = FieldType.text, analyzer = "ik_max_word", searchAnalyzer = "ik_max_word")
    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public Long getSendDate() {
        return sendDate;
    }

    public void setSendDate(Long sendDate) {
        this.sendDate = sendDate;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
