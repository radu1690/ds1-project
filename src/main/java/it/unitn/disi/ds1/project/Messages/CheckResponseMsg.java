package it.unitn.disi.ds1.project.Messages;

public class CheckResponseMsg extends Message {
    public CheckResponseMsg(Integer dataId, String requestId) {
        super(dataId, requestId);
    }
}