package it.unitn.disi.ds1.project;

import akka.actor.ActorRef;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class L2 extends Cache{

    private ActorRef fatherL1;
    private boolean crashedFather;
    //for each request, keep track of the request message to reuse it if the father crashes
    protected HashMap<String, Messages.Message> requestsMessages;
    private HashMap<String, Boolean> checkMsgAnswers;
    public L2(ActorRef database, ActorRef fatherL1) {
        super(database);
        this.fatherL1 = fatherL1;
        requestsMessages = new HashMap<>();
        this.crashedFather = false;
        checkMsgAnswers = new HashMap<>();
    }


    static public Props props(ActorRef database, ActorRef fatherL1) {
        return Props.create(L2.class, () -> new L2(database, fatherL1));
    }



    protected void onReadRequestMsg(Messages.ReadRequestMsg msg, ActorRef sender) {
        Messages.simulateDelay();
        if(sender == null){
            sender = getSender();
        }
        if(gonnaCrash(Messages.CrashType.ReadRequest, Messages.CrashTime.MessageReceived)){
            return;
        }


        //if the message is in cache, simply return it
        if(this.data.get(msg.dataId) != null){
            //if a lock is present, delay request
            if(isLocked(msg)){
                this.requestsActors.put(msg.requestId, sender);
                pendingReads.add(msg);
                return;
            }
//            say("L2: data present in cache");
            Messages.ReadResponseMsg response = new Messages.ReadResponseMsg(msg.dataId, data.get(msg.dataId), msg.requestId);
            sender.tell(response, getSelf());
            return;
        }

        //otherwise save the client and ask the L1 cache or the database
        this.requestsActors.put(msg.requestId, sender);
        sendMessageAndAddTimeout(msg);
        gonnaCrash(Messages.CrashType.ReadRequest, Messages.CrashTime.MessageProcessed);
    }

    private void onReadResponseMsg(Messages.ReadResponseMsg msg) {
        Messages.simulateDelay();
        if(gonnaCrash(Messages.CrashType.ReadResponse, Messages.CrashTime.MessageReceived)){
            return;
        }
        //check requests, remove it and update the data
        if(requestsActors.get(msg.requestId) != null){
            if(!Objects.equals(data.get(msg.dataId), msg.value)){
                say(": updated dataId " + msg.dataId + " with value " + msg.value);
                this.data.put(msg.dataId, msg.value);
            }
            requestsActors.remove(msg.requestId).tell(msg, getSelf());
            requestsMessages.remove(msg.requestId);
        }
        gonnaCrash(Messages.CrashType.ReadResponse, Messages.CrashTime.MessageProcessed);
    }

    private void onWriteRequestMsg(Messages.WriteRequestMsg msg) {
        Messages.simulateDelay();
        if(gonnaCrash(Messages.CrashType.WriteRequest, Messages.CrashTime.MessageReceived)){
            return;
        }
        if(servedWrites.contains(msg.requestId)){
            System.out.println(getSelf().path().name()+ ": Request already served, not writing again");
            Messages.WriteResponseMsg response = new Messages.WriteResponseMsg(msg.dataId, data.get(msg.dataId), msg.requestId);
            getSender().tell(response, getSelf());
            return;
        }
        this.requestsActors.put(msg.requestId, getSender());
        sendMessageAndAddTimeout(msg);
        gonnaCrash(Messages.CrashType.WriteRequest, Messages.CrashTime.MessageProcessed);
    }

    private void onWriteResponseMsg(Messages.WriteResponseMsg msg) {
        Messages.simulateDelay();
        if(gonnaCrash(Messages.CrashType.WriteResponse, Messages.CrashTime.MessageReceived)){
            return;
        }

        if(getSender() == this.database){
            crashedFather = true;
        }

        //update the data only if it is present or if the write request was on this cache
        if(this.data.get(msg.dataId) != null || this.requestsActors.get(msg.requestId) != null){
            if(!Objects.equals(data.get(msg.dataId), msg.currentValue)) {
                //update if data is different
                this.data.put(msg.dataId, msg.currentValue);
                System.out.println(getSelf().path().name() + ": updated dataId " + msg.dataId + " with value " + msg.currentValue);
            }
            if(this.requestsMessages.get(msg.requestId)!=null){
                this.requestsMessages.remove(msg.requestId);
            }
            //set write request as served
            this.servedWrites.add(msg.requestId);
        }
        //if a client requested this data, remove it from requests and send it
        if(this.requestsActors.get(msg.requestId) != null) {
            this.requestsActors.remove(msg.requestId).tell(msg, getSelf());
        }
        //remove eventual lock and process pending reads
        removeLock(msg);
        processReads();
        gonnaCrash(Messages.CrashType.WriteResponse, Messages.CrashTime.MessageProcessed);
    }

    private void onCritReadRequestMsg(Messages.CritReadRequestMsg msg) {
        Messages.simulateDelay();
        if(gonnaCrash(Messages.CrashType.CritReadRequest, Messages.CrashTime.MessageReceived)){
            return;
        }
        this.requestsActors.put(msg.requestId, getSender());
        sendMessageAndAddTimeout(msg);
        gonnaCrash(Messages.CrashType.CritReadRequest, Messages.CrashTime.MessageProcessed);
    }

    private void onCritWriteRequestMsg(Messages.CritWriteRequestMsg msg){
        Messages.simulateDelay();
        if(gonnaCrash(Messages.CrashType.CritWriteRequest, Messages.CrashTime.MessageReceived)){
            return;
        }
        if(servedWrites.contains(msg.requestId)){
            System.out.println(getSelf().path().name()+ ": Request already served, not writing again");
            Messages.WriteResponseMsg response = new Messages.WriteResponseMsg(msg.dataId, data.get(msg.dataId), msg.requestId);
            getSender().tell(response, getSelf());
            return;
        }
        this.requestsActors.put(msg.requestId, getSender());
        sendMessageAndAddTimeout(msg);
        gonnaCrash(Messages.CrashType.CritWriteRequest, Messages.CrashTime.MessageProcessed);
    }

    private void onFlushRequestMsg(Messages.FlushRequestMsg msg){
        Messages.simulateDelay();
        if(gonnaCrash(Messages.CrashType.FlushRequest, Messages.CrashTime.MessageReceived)){
            return;
        }
        setLock(msg);
        //answer back
        Messages.FlushResponseMsg flushResponse = new Messages.FlushResponseMsg(msg.dataId, msg.requestId);
        getSender().tell(flushResponse, getSelf());
        gonnaCrash(Messages.CrashType.FlushRequest, Messages.CrashTime.MessageProcessed);
    }

    private void onRefillRequestMsg(Messages.RefillRequestMsg msg) {
        Messages.simulateDelay();
        if(gonnaCrash(Messages.CrashType.RefillRequest, Messages.CrashTime.MessageReceived)){
            return;
        }
        if(this.data.get(msg.dataId) != null){
            System.out.println(getSelf().path().name()+ ": refilled dataId "+msg.dataId + " with value " + msg.value+", old value was: "+data.get(msg.dataId));
            this.data.put(msg.dataId, msg.value);
            //since the data is updated, add write to servedWrites (useful if another l2 crashes)
            servedWrites.add(msg.requestId);
        }
        removeLock(msg);
        if(!isLocked(msg)){
            processReads();
        }
        //no need to tell parent (db does not add timeout on L2)
        gonnaCrash(Messages.CrashType.RefillRequest, Messages.CrashTime.MessageProcessed);
    }

    private void sendMessageAndAddTimeout(Messages.Message m){
        requestsMessages.put(m.requestId, m);
        if(this.crashedFather){
            this.database.tell(m, getSelf());
            //no timeout if father is db
        }else{
            this.fatherL1.tell(m, getSelf());
            getContext().system().scheduler().scheduleOnce(
                    Duration.create(Messages.msgTimeoutMs, TimeUnit.MILLISECONDS),  // how frequently generate them
                    getSelf(),                                          // destination actor reference
                    new Messages.SelfTimeoutMsg(m.dataId, m.requestId, this.fatherL1),             // the message to send
                    getContext().system().dispatcher(),                 // system dispatcher
                    getSelf()                                           // source of the message (myself)
            );
        }

    }

    private void onSelfTimeoutMsg(Messages.SelfTimeoutMsg msg){
        if(requestsMessages.get(msg.requestId)==null){
            //the request has been served
            return;
        }
        //request still not served, need to check if father crashed:
        if(!crashedFather){
            this.checkMsgAnswers.put(msg.requestId, false);
            Messages.CheckMsg m = new Messages.CheckMsg(msg.dataId, msg.requestId);
            msg.receiver.tell(m, getSelf());
            getContext().system().scheduler().scheduleOnce(
                    Duration.create(Messages.checkTimeoutMs, TimeUnit.MILLISECONDS),  // how frequently generate them
                    getSelf(),                                          // destination actor reference
                    new Messages.CheckTimeoutMsg(msg.dataId, msg.requestId, msg.receiver),             // the message to send
                    getContext().system().dispatcher(),                 // system dispatcher
                    getSelf()                                           // source of the message (myself)
            );
        }
    }

    private void onCheckResponseMsg(Messages.CheckResponseMsg msg){
        Messages.simulateDelay();
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
        Serializable m = requestsMessages.get(msg.requestId);
        if(checkMsgAnswers.get(msg.requestId)){
            //cache is still online, need to wait
            //refresh selftimeout
            refreshSelfTimeout(msg.requestId, msg.receiver);
        }else{
            //father L1 crashed
            sayError("CRASH DETECTED ON: "+ this.fatherL1.path().name());
            //set father as crashed
            this.crashedFather = true;
            //contact the database, timeout not needed
            this.database.tell(m, getSelf());
        }
    }

    private void onCrashedFatherMsg(Messages.CrashedFather msg) {
        Messages.simulateDelay();
        say("Received crashed father msg");
        crashedFather = true;
    }

    private void refreshSelfTimeout(String requestId, ActorRef receiver){
        getContext().system().scheduler().scheduleOnce(
                Duration.create(Messages.msgTimeoutMs, TimeUnit.MILLISECONDS),  // how frequently generate them
                getSelf(),                                          // destination actor reference
                new Messages.SelfTimeoutMsg(null, requestId, receiver),             // the message to send
                getContext().system().dispatcher(),                 // system dispatcher
                getSelf()                                           // source of the message (myself)
        );
    }

    @Override
    protected void onRecoveryMsg(Messages.RecoveryMsg msg){
        data = new HashMap<>();
        requestsActors = new HashMap<>();
        requestsMessages = new HashMap<>();
        pendingReads = new ArrayList<>();
        nextCrash = Messages.CrashType.NONE;
        nextCrashWhen = Messages.CrashTime.NONE;
        servedWrites = new HashSet<>();
//        locks = new HashMap<>();
        locks = new HashSet<>();
        checkMsgAnswers = new HashMap<>();

        getContext().become(createReceive());

        //CONTACT FATHER/DB
        Messages.ChildReconnectedMsg m = new Messages.ChildReconnectedMsg();
        if(!crashedFather){
            fatherL1.tell(m, getSelf());
        }else{
            database.tell(m, getSelf());
        }
    }

    private void onReadRequestMsgMatch(Messages.ReadRequestMsg msg){
        onReadRequestMsg(msg, null);
    }
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Messages.ReadRequestMsg.class, this::onReadRequestMsgMatch)
                .match(Messages.ReadResponseMsg.class, this::onReadResponseMsg)
                .match(Messages.WriteResponseMsg.class, this::onWriteResponseMsg)
                .match(Messages.WriteRequestMsg.class, this::onWriteRequestMsg)
                .match(Messages.CritReadRequestMsg.class, this::onCritReadRequestMsg)
                .match(Messages.CheckConsistencyMsg.class, this::onCheckConsistencyMsg)
                .match(Messages.CritWriteRequestMsg.class, this::onCritWriteRequestMsg)
                .match(Messages.RefillRequestMsg.class, this::onRefillRequestMsg)
                .match(Messages.FlushRequestMsg.class, this::onFlushRequestMsg)
                .match(Messages.CrashMsg.class, this::onCrashMsg)
                .match(Messages.CheckMsg.class, this::onCheckMsg)
                .match(Messages.SelfTimeoutMsg.class, this::onSelfTimeoutMsg)
                .match(Messages.CheckResponseMsg.class, this::onCheckResponseMsg)
                .match(Messages.CheckTimeoutMsg.class, this::onCheckTimeoutMsg)
                .match(Messages.CrashedFather.class, this::onCrashedFatherMsg)
                .match(Messages.RecoveryMsg.class, this::onRecoveryMsg)
                .build();
    }

    protected void onCheckMsg(Messages.CheckMsg msg){
        say("Received checkMsg from: "+getSender().path().name());
        ActorRef sender = getSender();
        Messages.CheckResponseMsg m = new Messages.CheckResponseMsg(msg.dataId, msg.requestId);
        sender.tell(m, getSelf());
    }
}
