package it.unitn.disi.ds1.project.Messages;

import akka.actor.ActorRef;

/**
 * Timeout message to check if the operation ended
 */
public class SelfTimeoutMsg extends Message {
    public final ActorRef receiver;
    public SelfTimeoutMsg(Integer dataId, String requestId, ActorRef receiver) {
        super(dataId, requestId);
        this.receiver = receiver;
    }
}
