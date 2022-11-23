package it.unitn.disi.ds1.project.Messages;

public class ReadResponseMsg extends Message {
    public final Integer value;
    public ReadResponseMsg(Integer dataId, Integer value, String requestId) {
        super(dataId, requestId);
        this.value = value;
    }
}
