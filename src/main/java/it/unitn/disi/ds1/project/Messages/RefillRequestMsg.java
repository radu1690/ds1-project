package it.unitn.disi.ds1.project.Messages;

public class RefillRequestMsg extends Message {
    public final Integer value;
    public RefillRequestMsg(Integer dataId, Integer value, String requestId) {
        super(dataId, requestId);
        this.value = value;
    }
}
