package it.unitn.disi.ds1.project.Messages;

/**
 * Message used to check if an actor (l1 or l2) is still online
 */
public class CheckMsg extends Message {
    public CheckMsg(Integer dataId, String requestId) {
        super(dataId, requestId);
    }
}