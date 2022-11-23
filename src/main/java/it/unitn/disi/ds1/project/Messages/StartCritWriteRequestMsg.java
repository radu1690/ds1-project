package it.unitn.disi.ds1.project.Messages;

import akka.actor.ActorRef;

//sent from main to client
public class StartCritWriteRequestMsg extends Message {
    public final Integer value;
    public final ActorRef l2;
    public StartCritWriteRequestMsg(Integer dataId, Integer value, ActorRef l2) {
        super(dataId, null);
        this.value = value;
        this.l2 = l2;
    }
    public StartCritWriteRequestMsg(Integer dataId, Integer value) {
        super(dataId, null);
        this.value = value;
        this.l2 = null;
    }
}