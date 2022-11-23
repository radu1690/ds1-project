package it.unitn.disi.ds1.project.Messages;

import akka.actor.ActorRef;

//sent from main to client
public class StartReadRequestMsg extends Message {
    public final ActorRef l2;
    public StartReadRequestMsg(Integer dataId, ActorRef l2) {
        super(dataId, null);
        this.l2 = l2;
    }
    public StartReadRequestMsg(Integer dataId) {
        super(dataId, null);
        this.l2 = null;
    }
}
