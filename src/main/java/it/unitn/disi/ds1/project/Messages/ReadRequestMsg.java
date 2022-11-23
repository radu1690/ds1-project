package it.unitn.disi.ds1.project.Messages;

import java.util.UUID;

public class ReadRequestMsg extends Message {
    public ReadRequestMsg(Integer dataId) {
        super(dataId, UUID.randomUUID().toString());
    }
}
