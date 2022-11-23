package it.unitn.disi.ds1.project.Messages;

import akka.actor.ActorRef;

/**
 * Timeout message to verify that a checkMsg was received
 */
public class CheckTimeoutMsg extends Message {
    public final ActorRef receiver;
    public CheckTimeoutMsg(Integer dataId, String requestId, ActorRef receiver){
        super(dataId, requestId);
        this.receiver = receiver;
    }
}
