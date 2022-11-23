package it.unitn.disi.ds1.project;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.project.Messages.*;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Client extends AbstractActor {

    private List<ActorRef> listL2;
    //keep track of request messages and reuse them if a cache crashes. Delete them when the response arrives
    private HashMap<String, Message> requestsMessages;
    //keep track of checkMsg responses
    private HashMap<String, Boolean> checkMsgAnswers;
    private Random random;
    private ArrayList<Message> requests;
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
        Message
                msg = requests.remove(0);
        sendMessageAndAddTimeout(msg, msg.requestId, destinationActors.get(msg.requestId));
    }
    private void addMsgToQueue(Message
                                       msg, ActorRef destination){
        destinationActors.put(msg.requestId, destination);
        requests.add(msg);
    }

    private void onClientInitializationMsg(ClientInitializationMsg msg) {
        this.listL2 = msg.listL2;
        //System.out.println(this.listL2);
    }



    private void onStartReadRequestMsg(StartReadRequestMsg msg) {
        ReadRequestMsg m = new ReadRequestMsg(msg.dataId);
        ActorRef l2 = msg.l2;
        if(l2 == null){
            l2 = getRandomL2();
        }
        addMsgToQueue(m, l2);
        processRequest();
//        sendMessageAndAddTimeout(m, m.requestId, l2);
    }

    private void onReadResponseMsg(ReadResponseMsg msg) {
        System.out.println(getSelf().path().name()+" received read response with dataId "+ msg.dataId + " and value "+msg.value+" from "+getSender().toString());
        //requests.remove(msg.requestId);
        requestsMessages.remove(msg.requestId);
        waitingResponse = false;
        processRequest();
    }

    private void onStartWriteRequestMsg(StartWriteRequestMsg msg) {
        WriteRequestMsg m = new WriteRequestMsg(msg.dataId, msg.value);
        ActorRef l2 = msg.l2;
        if(l2 == null){
            l2 = getRandomL2();
        }
//        sendMessageAndAddTimeout(m, m.requestId, l2);
        addMsgToQueue(m, l2);
        processRequest();
    }

    private void onWriteResponseMsg(WriteResponseMsg msg){
        say(getSelf().path().name()+" received write response with dataId "+ msg.dataId + " and value " + msg.currentValue+" from "+getSender().path().name());
//        requests.remove(msg.requestId);
        requestsMessages.remove(msg.requestId);
        waitingResponse = false;
        processRequest();
    }

    private void onStartCritReadRequestMsg(StartCritReadRequestMsg msg) {
        CritReadRequestMsg m = new CritReadRequestMsg(msg.dataId);
        ActorRef l2 = msg.l2;
        if(l2 == null){
            l2 = getRandomL2();
        }
//        sendMessageAndAddTimeout(m, m.requestId, l2);
        addMsgToQueue(m, l2);
        processRequest();
    }

    private void onStartCritWriteRequestMsg(StartCritWriteRequestMsg msg){
        CritWriteRequestMsg m = new CritWriteRequestMsg(msg.dataId, msg.value);
        ActorRef l2 = msg.l2;
        if(l2 == null){
            l2 = getRandomL2();
        }
//        say("received critWrite request with dataId "+ msg.dataId + " and value " + msg.value);
//        sendMessageAndAddTimeout(m, m.requestId, l2);
        addMsgToQueue(m, l2);
        processRequest();
    }

    private void onSelfTimeoutMsg(SelfTimeoutMsg msg){
        if(requestsMessages.get(msg.requestId)==null){
            //the request has been served
            return;
        }
        //request still not served, need to check if cache crashed:
        this.checkMsgAnswers.put(msg.requestId, false);
        CheckMsg m = new CheckMsg(msg.dataId, msg.requestId);
        msg.receiver.tell(m, getSelf());
        getContext().system().scheduler().scheduleOnce(
                Duration.create(100, TimeUnit.MILLISECONDS),  // how frequently generate them
                getSelf(),                                          // destination actor reference
                new CheckTimeoutMsg(msg.dataId, msg.requestId, msg.receiver),             // the message to send
                getContext().system().dispatcher(),                 // system dispatcher
                getSelf()                                           // source of the message (myself)
        );

    }
    private void onCheckResponseMsg(CheckResponseMsg msg){
        if(requestsMessages.get(msg.requestId) == null){
            //request served
            return;
        }
        checkMsgAnswers.put(msg.requestId, true);
    }
    private void onCheckTimeoutMsg(CheckTimeoutMsg msg){
        if(requestsMessages.get(msg.requestId) == null){
            //request served
            return;
        }
        Message
                m = requestsMessages.get(msg.requestId);
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

    private void sendMessageAndAddTimeout(Message
                                                  m, String requestId, ActorRef destination){
        requestsMessages.put(requestId, m);
        destination.tell(m, getSelf());
        getContext().system().scheduler().scheduleOnce(
                Duration.create(400, TimeUnit.MILLISECONDS),  // how frequently generate them
                getSelf(),                                          // destination actor reference
                new SelfTimeoutMsg(m.dataId, requestId, destination),             // the message to send
                getContext().system().dispatcher(),                 // system dispatcher
                getSelf()                                           // source of the message (myself)
        );
    }

    private void refreshSelfTimeout(String requestId, ActorRef receiver){
        getContext().system().scheduler().scheduleOnce(
                Duration.create(400, TimeUnit.MILLISECONDS),  // how frequently generate them
                getSelf(),                                          // destination actor reference
                new SelfTimeoutMsg(requestsMessages.get(requestId).dataId,requestId, receiver),             // the message to send
                getContext().system().dispatcher(),                 // system dispatcher
                getSelf()                                           // source of the message (myself)
        );
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ClientInitializationMsg.class,  this::onClientInitializationMsg)
                .match(StartReadRequestMsg.class, this::onStartReadRequestMsg)
                .match(ReadResponseMsg.class, this::onReadResponseMsg)
                .match(StartWriteRequestMsg.class, this::onStartWriteRequestMsg)
                .match(WriteResponseMsg.class, this::onWriteResponseMsg)
                .match(StartCritReadRequestMsg.class, this::onStartCritReadRequestMsg)
                .match(StartCritWriteRequestMsg.class, this::onStartCritWriteRequestMsg)
                .match(SelfTimeoutMsg.class, this::onSelfTimeoutMsg)
                .match(CheckResponseMsg.class, this::onCheckResponseMsg)
                .match(CheckTimeoutMsg.class, this::onCheckTimeoutMsg)
                .build();
    }
    private void say(String text){
        System.out.println(getSelf().path().name()+": "+text);
    }
    protected void sayError(String text){
        System.err.println(getSelf().path().name()+": "+text);
    }

}
