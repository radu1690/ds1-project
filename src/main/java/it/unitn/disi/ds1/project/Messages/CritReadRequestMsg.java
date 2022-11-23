package it.unitn.disi.ds1.project.Messages;

import java.util.UUID;

public class CritReadRequestMsg extends Message {
    public CritReadRequestMsg(Integer dataId) {
        super(dataId, UUID.randomUUID().toString());
    }
}
