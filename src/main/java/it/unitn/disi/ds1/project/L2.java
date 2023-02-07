package it.unitn.disi.ds1.project;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.project.Messages.*;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class L2 extends Cache{

    private ActorRef fatherL1;
    private boolean crashedFather;
    //for each request, keep track of the request message to reuse it if the father crashes
    protected HashMap<String, Message> requestsMessages;
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



    protected void onReadRequestMsg(ReadRequestMsg msg, ActorRef sender) {
        Common.simulateDelay();
        if(sender == null){
            sender = getSender();
        }
        if(gonnaCrash(Common.CrashType.ReadRequest, Common.CrashTime.MessageReceived)){
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
            ReadResponseMsg response = new ReadResponseMsg(msg.dataId, data.get(msg.dataId), msg.requestId);
            sender.tell(response, getSelf());
            return;
        }

        //otherwise save the client and ask the L1 cache or the database
        this.requestsActors.put(msg.requestId, sender);
        sendMessageAndAddTimeout(msg);
        gonnaCrash(Common.CrashType.ReadRequest, Common.CrashTime.MessageProcessed);
    }

    private void onReadResponseMsg(ReadResponseMsg msg) {
        Common.simulateDelay();
        if(gonnaCrash(Common.CrashType.ReadResponse, Common.CrashTime.MessageReceived)){
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
        checkMsgAnswers.remove(msg.requestId);
        gonnaCrash(Common.CrashType.ReadResponse, Common.CrashTime.MessageProcessed);
    }

    private void onWriteRequestMsg(WriteRequestMsg msg) {
        Common.simulateDelay();
        if(gonnaCrash(Common.CrashType.WriteRequest, Common.CrashTime.MessageReceived)){
            return;
        }
        if(servedWrites.contains(msg.requestId)){
            System.out.println(getSelf().path().name()+ ": Request already served, not writing again");
            WriteResponseMsg response = new WriteResponseMsg(msg.dataId, data.get(msg.dataId), msg.requestId);
            getSender().tell(response, getSelf());
            return;
        }
        this.requestsActors.put(msg.requestId, getSender());
        sendMessageAndAddTimeout(msg);
        gonnaCrash(Common.CrashType.WriteRequest, Common.CrashTime.MessageProcessed);
    }

    private void onWriteResponseMsg(WriteResponseMsg msg) {
//        sayError("Received write response from: "+getSender().path().name());
        Common.simulateDelay();
        if(gonnaCrash(Common.CrashType.WriteResponse, Common.CrashTime.MessageReceived)){
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
//            sayError("Sending response back to client: "+this.requestsActors.get(msg.requestId).path().name());
            this.requestsActors.remove(msg.requestId).tell(msg, getSelf());
        }
        //remove eventual lock and process pending reads
        removeLock(msg);
        checkMsgAnswers.remove(msg.requestId);
        processReads();
        gonnaCrash(Common.CrashType.WriteResponse, Common.CrashTime.MessageProcessed);
    }

    private void onCritReadRequestMsg(CritReadRequestMsg msg) {
        Common.simulateDelay();
        if(gonnaCrash(Common.CrashType.CritReadRequest, Common.CrashTime.MessageReceived)){
            return;
        }
        this.requestsActors.put(msg.requestId, getSender());
        sendMessageAndAddTimeout(msg);
        gonnaCrash(Common.CrashType.CritReadRequest, Common.CrashTime.MessageProcessed);
    }

    private void onCritWriteRequestMsg(CritWriteRequestMsg msg){
        Common.simulateDelay();
        if(gonnaCrash(Common.CrashType.CritWriteRequest, Common.CrashTime.MessageReceived)){
            return;
        }
        if(servedWrites.contains(msg.requestId)){
            System.out.println(getSelf().path().name()+ ": Request already served, not writing again");
            WriteResponseMsg response = new WriteResponseMsg(msg.dataId, data.get(msg.dataId), msg.requestId);
            getSender().tell(response, getSelf());
            return;
        }
        this.requestsActors.put(msg.requestId, getSender());
        sendMessageAndAddTimeout(msg);
        gonnaCrash(Common.CrashType.CritWriteRequest, Common.CrashTime.MessageProcessed);
    }

    private void onFlushRequestMsg(FlushRequestMsg msg){
        Common.simulateDelay();
        if(gonnaCrash(Common.CrashType.FlushRequest, Common.CrashTime.MessageReceived)){
            return;
        }
        setLock(msg);
        //answer back
        FlushResponseMsg flushResponse = new FlushResponseMsg(msg.dataId, msg.requestId);
        getSender().tell(flushResponse, getSelf());
        gonnaCrash(Common.CrashType.FlushRequest, Common.CrashTime.MessageProcessed);
    }

    private void onRefillRequestMsg(RefillRequestMsg msg) {
        Common.simulateDelay();
        if(gonnaCrash(Common.CrashType.RefillRequest, Common.CrashTime.MessageReceived)){
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
        gonnaCrash(Common.CrashType.RefillRequest, Common.CrashTime.MessageProcessed);
    }

    private void sendMessageAndAddTimeout(Message m){
        requestsMessages.put(m.requestId, m);
        if(this.crashedFather){
            this.database.tell(m, getSelf());
            //no timeout if father is db
        }else{
            this.fatherL1.tell(m, getSelf());
            getContext().system().scheduler().scheduleOnce(
                    Duration.create(Common.msgTimeoutMs, TimeUnit.MILLISECONDS),  // how frequently generate them
                    getSelf(),                                          // destination actor reference
                    new SelfTimeoutMsg(m.dataId, m.requestId, this.fatherL1),             // the message to send
                    getContext().system().dispatcher(),                 // system dispatcher
                    getSelf()                                           // source of the message (myself)
            );
        }

    }

    private void onSelfTimeoutMsg(SelfTimeoutMsg msg){
        if(requestsMessages.get(msg.requestId)==null){
            //the request has been served
            return;
        }
        //request still not served, need to check if father crashed:
        if(!crashedFather){
            this.checkMsgAnswers.put(msg.requestId, false);
            CheckMsg m = new CheckMsg(msg.dataId, msg.requestId);
            msg.receiver.tell(m, getSelf());
            getContext().system().scheduler().scheduleOnce(
                    Duration.create(Common.checkTimeoutMs, TimeUnit.MILLISECONDS),  // how frequently generate them
                    getSelf(),                                          // destination actor reference
                    new CheckTimeoutMsg(msg.dataId, msg.requestId, msg.receiver),             // the message to send
                    getContext().system().dispatcher(),                 // system dispatcher
                    getSelf()                                           // source of the message (myself)
            );
        }else{
            Serializable m = requestsMessages.get(msg.requestId);
            this.database.tell(m, getSelf());
        }
    }

    private void onCheckResponseMsg(CheckResponseMsg msg){
        Common.simulateDelay();
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

    private void onCrashedFatherMsg(CrashedFather msg) {
        Common.simulateDelay();
        say("Received crashed father msg");
        crashedFather = true;
    }

    private void refreshSelfTimeout(String requestId, ActorRef receiver){
        getContext().system().scheduler().scheduleOnce(
                Duration.create(Common.msgTimeoutMs, TimeUnit.MILLISECONDS),  // how frequently generate them
                getSelf(),                                          // destination actor reference
                new SelfTimeoutMsg(null, requestId, receiver),             // the message to send
                getContext().system().dispatcher(),                 // system dispatcher
                getSelf()                                           // source of the message (myself)
        );
    }

    @Override
    protected void onRecoveryMsg(RecoveryMsg msg){
        data = new HashMap<>();
        requestsActors = new HashMap<>();
        requestsMessages = new HashMap<>();
        pendingReads = new ArrayList<>();
        nextCrash = Common.CrashType.NONE;
        nextCrashWhen = Common.CrashTime.NONE;
        servedWrites = new HashSet<>();
//        locks = new HashMap<>();
        locks = new HashSet<>();
        checkMsgAnswers = new HashMap<>();

        getContext().become(createReceive());

        //CONTACT FATHER/DB
        ChildReconnectedMsg m = new ChildReconnectedMsg();
        if(!crashedFather){
            fatherL1.tell(m, getSelf());
        }else{
            database.tell(m, getSelf());
        }
    }

    private void onReadRequestMsgMatch(ReadRequestMsg msg){
        onReadRequestMsg(msg, null);
    }
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ReadRequestMsg.class, this::onReadRequestMsgMatch)
                .match(ReadResponseMsg.class, this::onReadResponseMsg)
                .match(WriteResponseMsg.class, this::onWriteResponseMsg)
                .match(WriteRequestMsg.class, this::onWriteRequestMsg)
                .match(CritReadRequestMsg.class, this::onCritReadRequestMsg)
                .match(CheckConsistencyMsg.class, this::onCheckConsistencyMsg)
                .match(CritWriteRequestMsg.class, this::onCritWriteRequestMsg)
                .match(RefillRequestMsg.class, this::onRefillRequestMsg)
                .match(FlushRequestMsg.class, this::onFlushRequestMsg)
                .match(CrashMsg.class, this::onCrashMsg)
                .match(CheckMsg.class, this::onCheckMsg)
                .match(SelfTimeoutMsg.class, this::onSelfTimeoutMsg)
                .match(CheckResponseMsg.class, this::onCheckResponseMsg)
                .match(CheckTimeoutMsg.class, this::onCheckTimeoutMsg)
                .match(CrashedFather.class, this::onCrashedFatherMsg)
                .match(RecoveryMsg.class, this::onRecoveryMsg)
                .build();
    }

    protected void onCheckMsg(CheckMsg msg){
        say("Received checkMsg from: "+getSender().path().name());
        ActorRef sender = getSender();
        CheckResponseMsg m = new CheckResponseMsg(msg.dataId, msg.requestId);
        sender.tell(m, getSelf());
    }
}
