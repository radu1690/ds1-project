package it.unitn.disi.ds1.project.Messages;

public class WriteResponseMsg extends Message {
    //current value of the data (might be different if write has already been served)
    public final Integer currentValue;
    public WriteResponseMsg(Integer dataId, Integer value, String requestId) {
        super(dataId, requestId);
        this.currentValue = value;
    }
}
