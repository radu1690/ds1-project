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
    //keep track of checkMsg responses
    private HashMap<String, Boolean> checkMsgAnswers;
    private Random random;
    private ArrayList<Messages.Message> requests;
    private boolean waitingResponse;
    private HashMap<String, ActorRef> destinationActors;

    public Client() {
        requests = new ArrayList<>();
        requestsMessages = new HashMap<>();
        random = new Random();
//        requestReceivers = new HashMap<>();
        checkMsgAnswers = new HashMap<>();
        waitingResponse = false;
        destinationActors = new HashMap<>();
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

    private void processRequest(){
        if(waitingResponse || requests.isEmpty()){
            return;
        }
        waitingResponse = true;
        Messages.Message msg = requests.remove(0);
        sendMessageAndAddTimeout(msg, msg.requestId, destinationActors.get(msg.requestId));
    }
    private void addMsgToQueue(Messages.Message msg, ActorRef destination){
        destinationActors.put(msg.requestId, destination);
        requests.add(msg);
    }

    private void onClientInitializationMsg(ClientInitializationMsg msg) {
        this.listL2 = msg.listL2;
        //System.out.println(this.listL2);
    }



    private void onStartReadRequestMsg(Messages.StartReadRequestMsg msg) {
        Messages.ReadRequestMsg m = new Messages.ReadRequestMsg(msg.dataId);
        ActorRef l2 = msg.l2;
        if(l2 == null){
            l2 = getRandomL2();
        }
        addMsgToQueue(m, l2);
        processRequest();
//        sendMessageAndAddTimeout(m, m.requestId, l2);
    }

    private void onReadResponseMsg(Messages.ReadResponseMsg msg) {
        System.out.println(getSelf().path().name()+" received read response with dataId "+ msg.dataId + " and value "+msg.value+" from "+getSender().toString());
        //requests.remove(msg.requestId);
        requestsMessages.remove(msg.requestId);
        waitingResponse = false;
        processRequest();
    }

    private void onStartWriteRequestMsg(Messages.StartWriteRequestMsg msg) {
        Messages.WriteRequestMsg m = new Messages.WriteRequestMsg(msg.dataId, msg.value);
        ActorRef l2 = msg.l2;
        if(l2 == null){
            l2 = getRandomL2();
        }
//        sendMessageAndAddTimeout(m, m.requestId, l2);
        addMsgToQueue(m, l2);
        processRequest();
    }

    private void onWriteResponseMsg(Messages.WriteResponseMsg msg){
        System.out.println(getSelf().path().name()+" received write response with dataId "+ msg.dataId + " and value " + msg.currentValue);
//        requests.remove(msg.requestId);
        requestsMessages.remove(msg.requestId);
        waitingResponse = false;
        processRequest();
    }

    private void onStartCritReadRequestMsg(Messages.StartCritReadRequestMsg msg) {
        Messages.CritReadRequestMsg m = new Messages.CritReadRequestMsg(msg.dataId);
        ActorRef l2 = msg.l2;
        if(l2 == null){
            l2 = getRandomL2();
        }
//        sendMessageAndAddTimeout(m, m.requestId, l2);
        addMsgToQueue(m, l2);
        processRequest();
    }

    private void onStartCritWriteRequestMsg(Messages.StartCritWriteRequestMsg msg){
        Messages.CritWriteRequestMsg m = new Messages.CritWriteRequestMsg(msg.dataId, msg.value);
        ActorRef l2 = msg.l2;
        if(l2 == null){
            l2 = getRandomL2();
        }
//        say("received critWrite request with dataId "+ msg.dataId + " and value " + msg.value);
//        sendMessageAndAddTimeout(m, m.requestId, l2);
        addMsgToQueue(m, l2);
        processRequest();
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
        checkMsgAnswers.put(msg.requestId, true);
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
            //refresh checkmsg
            refreshSelfTimeout(msg.requestId, msg.receiver);
        }else{
            //cache crashed, need to choose another cache
            sayError("CRASH DETECTED ON: "+ msg.receiver.path().name());
            ActorRef l2 = getRandomL2();
            sayError("NEW CACHE CHOSEN: "+l2.path().name());
            sendMessageAndAddTimeout(m, msg.requestId, l2);
        }

    }

    private ActorRef getRandomL2(){
        int index = random.nextInt(listL2.size());
        return listL2.get(index);
    }

    private void sendMessageAndAddTimeout(Messages.Message m, String requestId, ActorRef destination){
        requestsMessages.put(requestId, m);
        destination.tell(m, getSelf());
        getContext().system().scheduler().scheduleOnce(
                Duration.create(400, TimeUnit.MILLISECONDS),  // how frequently generate them
                getSelf(),                                          // destination actor reference
                new Messages.SelfTimeoutMsg(m.dataId, requestId, destination),             // the message to send
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
    protected void sayError(String text){
        System.err.println(getSelf().path().name()+": "+text);
    }

}
