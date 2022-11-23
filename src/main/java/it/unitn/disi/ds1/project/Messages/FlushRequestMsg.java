package it.unitn.disi.ds1.project.Messages;

public class FlushRequestMsg extends Message {
    public FlushRequestMsg(Integer dataId, String requestId){
        super(dataId, requestId);
    }
}
