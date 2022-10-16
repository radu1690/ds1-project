package it.unitn.disi.ds1.project;

import akka.actor.ActorRef;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class L1 extends Cache{

    private List<ActorRef> cacheL2;
    private HashMap<String, HashSet<ActorRef>> flushChecks;
    private HashSet<ActorRef> crashedCaches;
    public L1(ActorRef database) {
        super(database);
        flushChecks = new HashMap<>();
        crashedCaches = new HashSet<>();
//        cacheL2 = new ArrayList<>();
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
            //but if a lock is present, delay request
            if(isLocked(msg)){
                this.requestsActors.put(msg.requestId, sender);
                pendingReads.add(msg);
                return;
            }
            Messages.ReadResponseMsg response = new Messages.ReadResponseMsg(msg.dataId, data.get(msg.dataId), msg.requestId);
            sender.tell(response, getSelf());
            return;
        }
        //otherwise save the sender of the request and ask the database
        this.requestsActors.put(msg.requestId, sender);
        this.database.tell(msg, getSelf());
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
                //if local data is different from message, update it and send refill to all children except requester
                say(": updated dataId " + msg.dataId + " with value " + msg.value + ", old value was: "+data.get(msg.dataId));
                this.data.put(msg.dataId, msg.value);
                Messages.RefillRequestMsg refill = new Messages.RefillRequestMsg(msg.dataId, msg.value, msg.requestId);
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
        gonnaCrash(Messages.CrashType.ReadResponse, Messages.CrashTime.MessageProcessed);
    }

    private void onWriteRequestMsg(Messages.WriteRequestMsg msg) {
        Messages.simulateDelay();
        ActorRef sender = getSender();
        if(gonnaCrash(Messages.CrashType.WriteRequest, Messages.CrashTime.MessageReceived)){
            return;
        }

        if(servedWrites.contains(msg.requestId)){
            System.out.println(getSelf().path().name()+ ": Request already served, not writing again");
            Messages.WriteResponseMsg response = new Messages.WriteResponseMsg(msg.dataId, data.get(msg.dataId), msg.requestId, false);
            sender.tell(response, getSelf());
            return;
        }
        this.requestsActors.put(msg.requestId, sender);
        this.database.tell(msg, getSelf());
        gonnaCrash(Messages.CrashType.WriteRequest, Messages.CrashTime.MessageProcessed);
    }


    private void onWriteResponseMsg(Messages.WriteResponseMsg msg) {
        Messages.simulateDelay();
        if(gonnaCrash(Messages.CrashType.WriteResponse, Messages.CrashTime.MessageReceived)){
            return;
        }
        if(!Objects.equals(data.get(msg.dataId), msg.currentValue)){
            //update if data is different
            this.data.put(msg.dataId, msg.currentValue);
            System.out.println(getSelf().path().name()+ ": updated dataId "+msg.dataId + " with value " + msg.currentValue);
            //send refill response to all child caches in order to update (expect the requester)
            Messages.RefillRequestMsg refill = new Messages.RefillRequestMsg(msg.dataId, msg.currentValue, msg.requestId);
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
        if(msg.afterFlush){
            removeLock(msg);
            processReads();
        }
        gonnaCrash(Messages.CrashType.WriteResponse, Messages.CrashTime.MessageProcessed);
    }



    private void onCritReadRequestMsg(Messages.CritReadRequestMsg msg) {
        Messages.simulateDelay();
        ActorRef sender = getSender();
        if(gonnaCrash(Messages.CrashType.CritReadRequest, Messages.CrashTime.MessageReceived)){
            return;
        }
        this.requestsActors.put(msg.requestId, sender);
        this.database.tell(msg, getSelf());
        gonnaCrash(Messages.CrashType.CritReadRequest, Messages.CrashTime.MessageProcessed);
    }

    private void onCritWriteRequestMsg(Messages.CritWriteRequestMsg msg){
//        say("Received critical write");
        Messages.simulateDelay();
        ActorRef sender = getSender();

        if(gonnaCrash(Messages.CrashType.CritWriteRequest, Messages.CrashTime.MessageReceived)){
            return;
        }
        if(servedWrites.contains(msg.requestId)){
            System.out.println(getSelf().path().name()+ ": Request already served, not writing again");
            Boolean valid = (data.get(msg.dataId).equals(msg.value));
            Messages.WriteResponseMsg response = new Messages.WriteResponseMsg(msg.dataId, data.get(msg.dataId), msg.requestId, false);
            sender.tell(response, getSelf());
            return;
        }
        this.requestsActors.put(msg.requestId, sender);
        this.database.tell(msg, getSelf());

        gonnaCrash(Messages.CrashType.CritWriteRequest, Messages.CrashTime.MessageProcessed);
    }

    private void onFlushRequestMsg(Messages.FlushRequestMsg msg){
        Messages.simulateDelay();
        if(gonnaCrash(Messages.CrashType.FlushRequest, Messages.CrashTime.MessageReceived)){
            return;
        }
        if(data.get(msg.dataId)!=null){
            //data is present, need to flush and tell children
            setLock(msg);
            System.out.println(getSelf().path().name()+ " flushed dataId"+msg.dataId);
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
                    Duration.create(200, TimeUnit.MILLISECONDS),  // how frequently generate them
                    getSelf(),                                          // destination actor reference
                    new Messages.CheckTimeoutMsg(msg.dataId, msg.requestId, null),             // the message to send
                    getContext().system().dispatcher(),                 // system dispatcher
                    getSelf()                                           // source of the message (myself)
            );
            //it will respond to db once all flushes from children will be received
        }else{
            //no data, it is safe to respond immediately to db
            Messages.FlushResponseMsg flushResponse = new Messages.FlushResponseMsg(msg.dataId, msg.requestId);
            this.database.tell(flushResponse, getSelf());
        }
        gonnaCrash(Messages.CrashType.FlushRequest, Messages.CrashTime.MessageProcessed);
    }

    private void onFlushResponseMsg(Messages.FlushResponseMsg msg){
        Messages.simulateDelay();
        HashSet<ActorRef> checks = flushChecks.get(msg.requestId);
        checks.remove(getSender());
        if(checks.isEmpty()){
            //no cache crashed
            Messages.FlushResponseMsg flushResponse = new Messages.FlushResponseMsg(msg.dataId, msg.requestId);
            this.database.tell(flushResponse, getSelf());
        }
    }

    private void onFlushCheckTimeoutMsg(Messages.CheckTimeoutMsg msg){
        HashSet<ActorRef> checks = flushChecks.get(msg.requestId);
        if(checks.isEmpty()){
            //no cache crashed
            return;
        }
        //if someone crashed, add it to crashedCaches
        crashedCaches.addAll(checks);
        System.err.println("CRASHED DETECTED "+checks);
        //respond to db
        Messages.FlushResponseMsg flushResponse = new Messages.FlushResponseMsg(msg.dataId, msg.requestId);
        this.database.tell(flushResponse, getSelf());
    }

    private void onRefillRequestMsg(Messages.RefillRequestMsg msg) {
        Messages.simulateDelay();
        if(gonnaCrash(Messages.CrashType.RefillRequest, Messages.CrashTime.MessageReceived)){
            return;
        }
        //update the data only if it is present
        if(this.data.get(msg.dataId) != null || this.requestsActors.get(msg.requestId) != null){
            System.out.println(getSelf().path().name()+ ": refilled dataId "+msg.dataId + " with value " + msg.value+", old value was: "+data.get(msg.dataId));
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
        Messages.RefillResponseMsg refillResponseMsg = new Messages.RefillResponseMsg(msg.dataId, msg.value, msg.requestId);
        this.database.tell(refillResponseMsg, getSelf());

        gonnaCrash(Messages.CrashType.RefillRequest, Messages.CrashTime.MessageProcessed);
    }

    @Override
    protected void onRecoveryMsg(Messages.RecoveryMsg msg){
        data = new HashMap<>();
        requestsActors = new HashMap<>();
        pendingReads = new ArrayList<>();
        nextCrash = Messages.CrashType.NONE;
        servedWrites = new HashSet<>();
        locks = new HashMap<>();
        getContext().become(createReceive());

        //CONTACT DB
        Messages.ChildReconnectedMsg m = new Messages.ChildReconnectedMsg();
        database.tell(m, getSelf());
    }

    private void onChildReconnectedMsg(Messages.ChildReconnectedMsg msg){
        ActorRef sender = getSender();
        boolean removed = crashedCaches.remove(getSender());
        say("Removed "+sender.path().name()+" from crashedCaches: "+ removed);
    }

    private void onReadRequestMsgMatch(Messages.ReadRequestMsg msg){
        onReadRequestMsg(msg, null);
    }


    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(L1.L1InitializationMsg.class,  this::onInitializationMsg)
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
                .match(Messages.CheckTimeoutMsg.class, this::onFlushCheckTimeoutMsg)
                .match(Messages.FlushResponseMsg.class, this::onFlushResponseMsg)
                .match(Messages.CheckMsg.class, this::onCheckMsg)
                .match(Messages.RecoveryMsg.class, this::onRecoveryMsg)
                .match(Messages.ChildReconnectedMsg.class, this::onChildReconnectedMsg)
                .build();
    }
}
