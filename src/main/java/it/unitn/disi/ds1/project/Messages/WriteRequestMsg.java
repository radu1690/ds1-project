package it.unitn.disi.ds1.project.Messages;

import java.util.UUID;

public class WriteRequestMsg extends Message {
    public final Integer value;
    public WriteRequestMsg(Integer dataId, Integer value) {
        super(dataId, UUID.randomUUID().toString());
        this.value = value;
    }
    public WriteRequestMsg(Integer dataId, Integer value, String requestId){
        super(dataId, requestId);
        this.value = value;
    }
}
