package it.unitn.disi.ds1.project.Messages;

public class RefillResponseMsg extends Message {
    public final Integer value;
    public RefillResponseMsg(Integer dataId, Integer value, String requestId) {
        super(dataId, requestId);
        this.value = value;
    }
}
