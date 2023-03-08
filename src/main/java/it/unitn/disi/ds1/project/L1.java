package it.unitn.disi.ds1.project;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.project.Messages.*;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class L1 extends Cache{

    private List<ActorRef> cacheL2;
    private HashMap<String, HashSet<ActorRef>> flushChecks;
    private HashSet<ActorRef> crashedCaches;
    private boolean recoveredAfterCrash;
    public L1(ActorRef database) {
        super(database);
        flushChecks = new HashMap<>();
        crashedCaches = new HashSet<>();
        recoveredAfterCrash = false;
    }

    static public Props props(ActorRef database) {
        return Props.create(L1.class, () -> new L1(database));
    }

    @Override
    public void preStart(){

    }

    public static class L1InitializationMsg implements Serializable {
        public final List<ActorRef> listL2;
        public L1InitializationMsg(List<ActorRef> listL2) {
            this.listL2 = Collections.unmodifiableList(new ArrayList<>(listL2));
        }
    }

    private void onInitializationMsg(L1.L1InitializationMsg msg) {
        this.cacheL2 = msg.listL2;
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
            //but if a lock is present, delay request
            if(isLocked(msg)){
                this.requestsActors.put(msg.requestId, sender);
                pendingReads.add(msg);
                return;
            }
            ReadResponseMsg response = new ReadResponseMsg(msg.dataId, data.get(msg.dataId), msg.requestId);
            sender.tell(response, getSelf());
            return;
        }
        //otherwise save the sender of the request and ask the database
        this.requestsActors.put(msg.requestId, sender);
        this.database.tell(msg, getSelf());
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
                //if local data is different from message, update it and send refill to all children except requester
                say(": updated dataId " + msg.dataId + " with value " + msg.value + ", old value was: "+data.get(msg.dataId));
                this.data.put(msg.dataId, msg.value);
                RefillRequestMsg refill = new RefillRequestMsg(msg.dataId, msg.value, msg.requestId);
                for(ActorRef l2 : cacheL2){
                    if(l2!=requestsActors.get(msg.requestId)){
                        l2.tell(refill, getSelf());
                        //no need to put a timeout, if L2 crash they lose their data
                    }
                }
            }
            //finally, send response to requester
            requestsActors.remove(msg.requestId).tell(msg, getSelf());
        }
        gonnaCrash(Common.CrashType.ReadResponse, Common.CrashTime.MessageProcessed);
    }

    private void onWriteRequestMsg(WriteRequestMsg msg) {
        Common.simulateDelay();
        ActorRef sender = getSender();
        if(gonnaCrash(Common.CrashType.WriteRequest, Common.CrashTime.MessageReceived)){
            return;
        }

        if(servedWrites.contains(msg.requestId)){
            System.out.println(getSelf().path().name()+ ": Request already served, not writing again");
            WriteResponseMsg response = new WriteResponseMsg(msg.dataId, data.get(msg.dataId), msg.requestId);
            sender.tell(response, getSelf());
            return;
        }
        this.requestsActors.put(msg.requestId, sender);
        this.database.tell(msg, getSelf());
        gonnaCrash(Common.CrashType.WriteRequest, Common.CrashTime.MessageProcessed);
    }


    private void onWriteResponseMsg(WriteResponseMsg msg) {
        Common.simulateDelay();
        if(gonnaCrash(Common.CrashType.WriteResponse, Common.CrashTime.MessageReceived)){
            return;
        }
        if(!Objects.equals(data.get(msg.dataId), msg.currentValue)){
            //update if data is different
            this.data.put(msg.dataId, msg.currentValue);
            System.out.println(getSelf().path().name()+ ": updated dataId "+msg.dataId + " with value " + msg.currentValue);
            //send refill response to all child caches in order to update (expect the requester)
            RefillRequestMsg refill = new RefillRequestMsg(msg.dataId, msg.currentValue, msg.requestId);
            for(ActorRef l2 : cacheL2){
                if(l2 != requestsActors.get(msg.requestId)){
                    l2.tell(refill, getSelf());
                    //no need to put a timeout, if L2 crash they lose their data
                }
            }
        }
        //lastly, send response to the cache that made the request
        //since the requester is last to receive the update, he will detect the crash
        //(database does not use timeout on write response)
        if(this.requestsActors.get(msg.requestId) != null) {
            this.requestsActors.remove(msg.requestId).tell(msg, getSelf());
        }
        //if this was a critical write, remove the lock and process pending reads
        //remove eventual lock and process pending reads
        removeLock(msg);
        processReads();
        gonnaCrash(Common.CrashType.WriteResponse, Common.CrashTime.MessageProcessed);
    }



    private void onCritReadRequestMsg(CritReadRequestMsg msg) {
        Common.simulateDelay();
        ActorRef sender = getSender();
        if(gonnaCrash(Common.CrashType.CritReadRequest, Common.CrashTime.MessageReceived)){
            return;
        }
        this.requestsActors.put(msg.requestId, sender);
        this.database.tell(msg, getSelf());
        gonnaCrash(Common.CrashType.CritReadRequest, Common.CrashTime.MessageProcessed);
    }

    private void onCritWriteRequestMsg(CritWriteRequestMsg msg){
//        say("Received critical write");
        Common.simulateDelay();
        ActorRef sender = getSender();

        if(gonnaCrash(Common.CrashType.CritWriteRequest, Common.CrashTime.MessageReceived)){
            return;
        }
        if(servedWrites.contains(msg.requestId)){
            System.out.println(getSelf().path().name()+ ": Request already served, not writing again");
            Boolean valid = (data.get(msg.dataId).equals(msg.value));
            WriteResponseMsg response = new WriteResponseMsg(msg.dataId, data.get(msg.dataId), msg.requestId);
            sender.tell(response, getSelf());
            return;
        }
        this.requestsActors.put(msg.requestId, sender);
        this.database.tell(msg, getSelf());

        gonnaCrash(Common.CrashType.CritWriteRequest, Common.CrashTime.MessageProcessed);
    }

    private void onFlushRequestMsg(FlushRequestMsg msg){
        Common.simulateDelay();
        if(gonnaCrash(Common.CrashType.FlushRequest, Common.CrashTime.MessageReceived)){
            return;
        }
        if(data.get(msg.dataId)!=null){
            //data is present, need to flush and tell children
            setLock(msg);
//            System.out.println(getSelf().path().name()+ " flushed dataId "+msg.dataId);
            HashSet<ActorRef> checks = new HashSet<ActorRef>();
            flushChecks.put(msg.requestId, checks);
            for(ActorRef L2: cacheL2){
                if(!crashedCaches.contains(L2)){
                    L2.tell(msg, getSelf());
                    checks.add(L2);
                }
            }
            //add timeout on children
            getContext().system().scheduler().scheduleOnce(
                    Duration.create(Common.checkTimeoutMs, TimeUnit.MILLISECONDS),  // how frequently generate them
                    getSelf(),                                          // destination actor reference
                    new CheckTimeoutMsg(msg.dataId, msg.requestId, null),             // the message to send
                    getContext().system().dispatcher(),                 // system dispatcher
                    getSelf()                                           // source of the message (myself)
            );
            //it will respond to db once all flushes from children will be received
        }else{
            //no data, it is safe to respond immediately to db
            FlushResponseMsg flushResponse = new FlushResponseMsg(msg.dataId, msg.requestId);
            this.database.tell(flushResponse, getSelf());
        }
        gonnaCrash(Common.CrashType.FlushRequest, Common.CrashTime.MessageProcessed);
    }

    private void onFlushResponseMsg(FlushResponseMsg msg){
        Common.simulateDelay();
        if(gonnaCrash(Common.CrashType.FlushResponse, Common.CrashTime.MessageReceived)){
            return;
        }
        HashSet<ActorRef> checks = flushChecks.get(msg.requestId);
        checks.remove(getSender());
        if(checks.isEmpty()){
            //no cache crashed
            FlushResponseMsg flushResponse = new FlushResponseMsg(msg.dataId, msg.requestId);
            this.database.tell(flushResponse, getSelf());
        }
        gonnaCrash(Common.CrashType.FlushResponse, Common.CrashTime.MessageProcessed);
    }

    private void onFlushCheckTimeoutMsg(CheckTimeoutMsg msg){
        HashSet<ActorRef> checks = flushChecks.get(msg.requestId);
        if(checks.isEmpty()){
            //no cache crashed
            return;
        }
        //if someone crashed, add it to crashedCaches
        crashedCaches.addAll(checks);
//        System.err.println("CRASHED DETECTED "+checks);
        sayError("Crashed children detected: "+checks);
        //respond to db
        FlushResponseMsg flushResponse = new FlushResponseMsg(msg.dataId, msg.requestId);
        this.database.tell(flushResponse, getSelf());
    }

    private void onRefillRequestMsg(RefillRequestMsg msg) {
        Common.simulateDelay();
        if(gonnaCrash(Common.CrashType.RefillRequest, Common.CrashTime.MessageReceived)){
            return;
        }
        //update the data only if it is present
        if(this.data.get(msg.dataId) != null || this.requestsActors.get(msg.requestId) != null){
            say(": refilled dataId "+msg.dataId + " with value " + msg.value+", old value was: "+data.get(msg.dataId));
            this.data.put(msg.dataId, msg.value);
            //refill children
            for(ActorRef l2 : cacheL2){
                l2.tell(msg, getSelf());
                //no need to put a timeout, if L2 crash they lose their data (the client will resend the write)
            }
        }
        removeLock(msg);
        if(!isLocked(msg)){
            processReads();
        }

        //send response to database only AFTER sending the refill to children!
        //this way, if L1 crashed after sending the refill, it's guaranteed that the children received the update
        RefillResponseMsg refillResponseMsg = new RefillResponseMsg(msg.dataId, msg.value, msg.requestId);
        this.database.tell(refillResponseMsg, getSelf());

        gonnaCrash(Common.CrashType.RefillRequest, Common.CrashTime.MessageProcessed);
    }

    @Override
    protected void onRecoveryMsg(RecoveryMsg msg){
//        data = new HashMap<>();
//        requestsActors = new HashMap<>();
//        pendingReads = new ArrayList<>();
//        nextCrash = Common.CrashType.NONE;
//        nextCrashWhen = Common.CrashTime.NONE;
//        servedWrites = new HashSet<>();
//        locks = new HashSet<>();
//        getContext().become(createReceive());
//        recoveredAfterCrash = true;
//        //CONTACT DB
//        ChildReconnectedMsg m = new ChildReconnectedMsg();
//        database.tell(m, getSelf());
    }

    private void onChildReconnectedMsg(ChildReconnectedMsg msg){
        ActorRef sender = getSender();
        boolean removed = crashedCaches.remove(getSender());
        say("Removed "+sender.path().name()+" from crashedCaches: "+ removed);
        //if this L1 cache recovered after a crash, L2 children should contact the database instead
        if(this.recoveredAfterCrash){
            CrashedFather m = new CrashedFather();
            getSender().tell(m, getSelf());
        }
    }

    private void onReadRequestMsgMatch(ReadRequestMsg msg){
        onReadRequestMsg(msg, null);
    }


    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(L1.L1InitializationMsg.class,  this::onInitializationMsg)
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
                .match(CheckTimeoutMsg.class, this::onFlushCheckTimeoutMsg)
                .match(FlushResponseMsg.class, this::onFlushResponseMsg)
                .match(CheckMsg.class, this::onCheckMsg)
                .match(RecoveryMsg.class, this::onRecoveryMsg)
                .match(ChildReconnectedMsg.class, this::onChildReconnectedMsg)
                .build();
    }
}
