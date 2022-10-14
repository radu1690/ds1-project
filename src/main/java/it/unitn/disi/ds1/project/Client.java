package it.unitn.disi.ds1.project;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Client extends AbstractActor {

    private List<ActorRef> listL2;
    //keep track of request messages and reuse them if a cache crashes. Delete them when the response arrives
    private HashMap<String, Messages.Message> requestsMessages;
    //keep track of receivers of msg
//    private HashMap<String, ActorRef> requestReceivers;
    //keep track of checkMsg responses
    private HashMap<String, Boolean> checkMsgAnswers;
    private Random random;

    public Client() {
        //requests = new HashSet<>();
        requestsMessages = new HashMap<>();
        random = new Random();
//        requestReceivers = new HashMap<>();
        checkMsgAnswers = new HashMap<>();
    }

    static public Props props() {
        return Props.create(Client.class, Client::new);
    }

    public static class ClientInitializationMsg implements Serializable {
        public final List<ActorRef> listL2;
        public ClientInitializationMsg(List<ActorRef> listL2) {
            this.listL2 = Collections.unmodifiableList(new ArrayList<>(listL2));
        }
    }

    private void onClientInitializationMsg(ClientInitializationMsg msg) {
        this.listL2 = msg.listL2;
        //System.out.println(this.listL2);
    }



    private void onStartReadRequestMsg(Messages.StartReadRequestMsg msg) {
        Messages.ReadRequestMsg m = new Messages.ReadRequestMsg(msg.dataId);
        sendMessageAndAddTimeout(m, m.requestId, msg.l2);
    }

    private void onReadResponseMsg(Messages.ReadResponseMsg msg) {
        System.out.println(getSelf().path().name()+" received read response with dataId "+ msg.dataId + " and value "+msg.value+" from "+getSender().toString());
        //requests.remove(msg.requestId);
        requestsMessages.remove(msg.requestId);
    }

    private void onStartWriteRequestMsg(Messages.StartWriteRequestMsg msg) {
        Messages.WriteRequestMsg m = new Messages.WriteRequestMsg(msg.dataId, msg.value);
//        requests.add(m.requestId);
        sendMessageAndAddTimeout(m, m.requestId, msg.l2);
    }

    private void onWriteResponseMsg(Messages.WriteResponseMsg msg){
        System.out.println(getSelf().path().name()+" received write response with dataId "+ msg.dataId + " and value " + msg.currentValue);
//        requests.remove(msg.requestId);
        requestsMessages.remove(msg.requestId);
    }

    private void onStartCritReadRequestMsg(Messages.StartCritReadRequestMsg msg) {
        Messages.CritReadRequestMsg m = new Messages.CritReadRequestMsg(msg.dataId);
//        requests.add(m.requestId);
        sendMessageAndAddTimeout(m, m.requestId, msg.l2);
    }

    private void onStartCritWriteRequestMsg(Messages.StartCritWriteRequestMsg msg){
        Messages.CritWriteRequestMsg m = new Messages.CritWriteRequestMsg(msg.dataId, msg.value);
//        requests.add(m.requestId);
        System.out.println(getSelf().path().name()+" received critWrite request with dataId "+ msg.dataId + " and value " + msg.value);
        sendMessageAndAddTimeout(m, m.requestId, msg.l2);
    }

    private void onSelfTimeoutMsg(Messages.SelfTimeoutMsg msg){
        if(requestsMessages.get(msg.requestId)==null){
            //the request has been served
            return;
        }
        //request still not served, need to check if cache crashed:
        this.checkMsgAnswers.put(msg.requestId, false);
        Messages.CheckMsg m = new Messages.CheckMsg(msg.dataId, msg.requestId);
        msg.receiver.tell(m, getSelf());
//        requestReceivers.get(msg.requestId).tell(m, getSelf());
        getContext().system().scheduler().scheduleOnce(
                Duration.create(100, TimeUnit.MILLISECONDS),  // how frequently generate them
                getSelf(),                                          // destination actor reference
                new Messages.CheckTimeoutMsg(msg.dataId, msg.requestId, msg.receiver),             // the message to send
                getContext().system().dispatcher(),                 // system dispatcher
                getSelf()                                           // source of the message (myself)
        );

    }
    private void onCheckResponseMsg(Messages.CheckResponseMsg msg){
        if(requestsMessages.get(msg.requestId) == null){
            //request served
            return;
        }
//        System.err.println("STILL NOT SERVED");

        checkMsgAnswers.put(msg.requestId, true);
//        System.out.println(checkMsgAnswers.get(msg.requestId));
    }
    private void onCheckTimeoutMsg(Messages.CheckTimeoutMsg msg){
        if(requestsMessages.get(msg.requestId) == null){
            //request served
            return;
        }


        Messages.Message m = requestsMessages.get(msg.requestId);
        //cache answered to the checkMsg but has not yet served the request
        if(checkMsgAnswers.get(msg.requestId)){
            //cache is still online, need to reschedule the check msg
//            receiver = requestReceivers.get(msg.requestId);
            //refresh checkmsg
            refreshSelfTimeout(msg.requestId, msg.receiver);
        }else{
            //cache crashed, need to choose another cache
//            System.err.println("CRASH DETECTED");
            say("CRASH DETECTED ON: "+ msg.receiver.path().name());
            int index = random.nextInt(listL2.size());
//            requestReceivers.put(msg.requestId, receiver);
            say("NEW CACHE CHOSEN: "+listL2.get(index).path().name());
            sendMessageAndAddTimeout(m, msg.requestId, listL2.get(index));
        }

    }

    private void sendMessageAndAddTimeout(Messages.Message m, String requestId, ActorRef receiver){
        requestsMessages.put(requestId, m);
//        requestReceivers.put(requestId, receiver);
        receiver.tell(m, getSelf());
        getContext().system().scheduler().scheduleOnce(
                Duration.create(400, TimeUnit.MILLISECONDS),  // how frequently generate them
                getSelf(),                                          // destination actor reference
                new Messages.SelfTimeoutMsg(m.dataId, requestId, receiver),             // the message to send
                getContext().system().dispatcher(),                 // system dispatcher
                getSelf()                                           // source of the message (myself)
        );
    }

    private void refreshSelfTimeout(String requestId, ActorRef receiver){
        getContext().system().scheduler().scheduleOnce(
                Duration.create(400, TimeUnit.MILLISECONDS),  // how frequently generate them
                getSelf(),                                          // destination actor reference
                new Messages.SelfTimeoutMsg(requestsMessages.get(requestId).dataId,requestId, receiver),             // the message to send
                getContext().system().dispatcher(),                 // system dispatcher
                getSelf()                                           // source of the message (myself)
        );
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ClientInitializationMsg.class,  this::onClientInitializationMsg)
                .match(Messages.StartReadRequestMsg.class, this::onStartReadRequestMsg)
                .match(Messages.ReadResponseMsg.class, this::onReadResponseMsg)
                .match(Messages.StartWriteRequestMsg.class, this::onStartWriteRequestMsg)
                .match(Messages.WriteResponseMsg.class, this::onWriteResponseMsg)
                .match(Messages.StartCritReadRequestMsg.class, this::onStartCritReadRequestMsg)
                .match(Messages.StartCritWriteRequestMsg.class, this::onStartCritWriteRequestMsg)
                .match(Messages.SelfTimeoutMsg.class, this::onSelfTimeoutMsg)
                .match(Messages.CheckResponseMsg.class, this::onCheckResponseMsg)
                .match(Messages.CheckTimeoutMsg.class, this::onCheckTimeoutMsg)
                .build();
    }
    private void say(String text){
        System.out.println(getSelf().path().name()+": "+text);
    }

}
