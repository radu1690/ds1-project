package it.unitn.disi.ds1.project.Messages;

import java.io.Serializable;

/**
 * Basic message
 */
public class Message implements Serializable {
    public final Integer dataId;
    public final String requestId;
    public Message(Integer dataId, String requestId) {
        this.dataId = dataId;
        this.requestId = requestId;
    }

}
