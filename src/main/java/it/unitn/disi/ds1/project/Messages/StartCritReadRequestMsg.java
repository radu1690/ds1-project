package it.unitn.disi.ds1.project.Messages;

import akka.actor.ActorRef;

//sent from main to client
public class StartCritReadRequestMsg extends Message {
    public final ActorRef l2;
    public StartCritReadRequestMsg(Integer dataId, ActorRef l2) {
        super(dataId, null);
        this.l2 = l2;
    }
    public StartCritReadRequestMsg(Integer dataId) {
        super(dataId, null);
        this.l2 = null;
    }
}
