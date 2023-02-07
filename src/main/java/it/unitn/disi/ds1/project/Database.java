package it.unitn.disi.ds1.project;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
//import it.unitn.disi.ds1.project.Messages;
import it.unitn.disi.ds1.project.Messages.*;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Database extends AbstractActor {
    /**
     * main data that is stored
     */
    private HashMap<Integer, Integer> data;
    /**
     * list of L1 caches
     */
    private List<ActorRef> cacheL1;
    /**
     * list of L2 caches for each L1 cache
     */
    private Map<ActorRef, ArrayList<ActorRef>> cacheL2;
    /**
     * father of each L2 cache
     */
    private HashMap<ActorRef, ActorRef> l2Fathers;
    /**
     * set of crashed caches (L1 will be left crashed forever, L2 can be removed if they recover)
     */
    private HashSet<ActorRef> crashedCaches;
    /**
     * keep track of the write request in order to not write a value two times.
     * in case of a write request that was already served, do not write and send back the current value
     */
    private Set<String> servedWrites;
    /**
     * set of actors that need to answer to a flush request (this will be checked to be empty, if it is not empty, the
     * crashed cache(s) is/are inside the set)
     */
    private HashMap<String, HashSet<ActorRef>> critWriteFlushes;
    /**
     * set of actors that need to answer to a fill request (again it will be checked to be empty...)
     */
    private HashMap<String, HashSet<ActorRef>> fillResponses;
    /**
     * set of actors that need to answer to a checkmsg if they did not answer to the flush request yet
     *     (again it will be checked to be empty...)
     */
    private HashMap<String, HashSet<ActorRef>> flushChecks;
    /**
     * for each request keep track of the actor that sent it
     */
    private HashMap<String, ActorRef> requestsActors;
    /**
     * some data is locked (can't be written or read) if a critical write on it has not yet been finished
     */
    private HashSet<Integer> locks;
    /**
     * store the write requests on locked data in order to process it later on
     */
    private ArrayList<Message> pendingRequestMessages;

    //data structure to check consistency
    private HashMap<Integer, ArrayList<ConsistencyCheck>> consistencyResponses;
    private int N_CACHES;

    public Database() {
        this.data = new HashMap<>();
        crashedCaches = new HashSet<>();
        servedWrites = new HashSet<>();
        critWriteFlushes = new HashMap<>();
        fillResponses = new HashMap<>();
        requestsActors = new HashMap<>();
        locks = new HashSet<>();
        pendingRequestMessages = new ArrayList<>();
        flushChecks = new HashMap<>();
        consistencyResponses = new HashMap<>();
    }

    static public Props props() {
        return Props.create(Database.class, Database::new);
    }

    @Override
    public void preStart(){

    }

    public static class DatabaseInitializationMsg implements Serializable {
        public final List<ActorRef> listL1;
        public final Map<ActorRef, ArrayList<ActorRef>> hashMapL2;
        public final HashMap<ActorRef, ActorRef> l2Fathers;
        public DatabaseInitializationMsg(List<ActorRef> listL1, HashMap<ActorRef, ArrayList<ActorRef>> hashMapL2, HashMap<ActorRef, ActorRef> l2Fathers) {
            this.listL1 = Collections.unmodifiableList(new ArrayList<>(listL1));
            this.hashMapL2 = Collections.unmodifiableMap(new HashMap<>(hashMapL2));
            this.l2Fathers = l2Fathers;
        }
    }

    public static class FlushTimeoutMsg extends Message {
        public FlushTimeoutMsg(String requestId, Integer dataId) {
            super(dataId, requestId);
        }
    }
    public static class FillTimeoutMsg extends Message {
        public FillTimeoutMsg(Integer dataId, String requestId) {
            super(dataId, requestId);
        }
    }

    private void onInitializationMsg(DatabaseInitializationMsg msg) {
        this.cacheL1 = msg.listL1;
        this.cacheL2 = msg.hashMapL2;
        this.l2Fathers = msg.l2Fathers;
        N_CACHES = cacheL1.size() + l2Fathers.size();
    }

    /**
     * Check if the requested data has a lock on it. If yes and addToPending is set to true: save the request in pendingRequestsMessages and save also
     * the actor.
     * @param msg
     * @param sender
     * @return true if the data has a lock, false otherwise.
     */
    public boolean checkLocks(Message msg, ActorRef sender, boolean addToPending){
        if(locks.contains(msg.dataId)){
            say(" lock detected on dataId "+ msg.dataId);
            if(addToPending){
                this.pendingRequestMessages.add(msg);
                this.requestsActors.put(msg.requestId, sender);
//                say(" added msg to pendingRequestMessages");
            }
            return true;
        }
        return false;
    }

    //this is the same for both normal read and critical read (critical read msg is mapped to this function)
    private void onReadRequestMsg(Message msg, ActorRef sender) {
        Common.simulateDelay();
        if(sender == null){
            sayError("Received readRequest on dataId: "+msg.dataId);
            sender = getSender();
        }

        checkCrashAndUpdate(sender);

        if(checkLocks(msg, sender, true)){
            return;
        }

        Integer value = this.data.get(msg.dataId);
        //send response with this data
        //no need for timeout
        ReadResponseMsg response = new ReadResponseMsg(msg.dataId, value, msg.requestId);
        sender.tell(response, getSelf());
    }


    private void onWriteRequestMsg(WriteRequestMsg msg, ActorRef sender) {
        Common.simulateDelay();
        if(sender == null){
            sender = getSender();
            sayError("Received writeRequest on dataId: "+msg.dataId+" and value: "+msg.value+" from "+sender.path().name());
        }
        checkCrashAndUpdate(sender);
        if(checkLocks(msg, sender, true)){
            return;
        }
//        say("VALUE RECEIVED: "+msg.value);

        //if the write request has already been served, no need to write again and update the other caches, simply send
        //back confirmation with the current value
        //this is the case when L1 or L2 crashes when receiving confirmation

        if(servedWrites.contains(msg.requestId)){
            say("Request already served, not writing again");
        }else{
            this.data.put(msg.dataId, msg.value);
        }
//        say("Current data is: "+this.data.get(msg.dataId));
        servedWrites.add(msg.requestId);
        WriteResponseMsg response = new WriteResponseMsg(msg.dataId, data.get(msg.dataId), msg.requestId);
        //send back to sender a writeResponseMsg
        sender.tell(response, getSelf());
        //for all the other caches, send a refillRequestMsg
        RefillRequestMsg refillRequestMsg = new RefillRequestMsg(msg.dataId, data.get(msg.dataId), msg.requestId);
        HashSet<ActorRef> fills = new HashSet<>();
        this.fillResponses.put(msg.requestId, fills);
        for(ActorRef L1 : this.cacheL1){
            //send to all L1 (except sender) that did not crash
            if(!crashedCaches.contains(L1)){
//                sayError("REFILLING L1: "+L1.path().name());
                if(L1 != sender){
                    fills.add(L1);
                    L1.tell(refillRequestMsg, getSelf());
                }
            }else{
                //this L1 crashed, need to send the response to its L2
//                sayError("L1 CRASHED: "+L1.path().name());
                for(ActorRef L2 : this.cacheL2.get(L1)){
                    if(!crashedCaches.contains(L2)){
                        //send only if not crashed
                        L2.tell(refillRequestMsg, getSelf());
                    }
                }
            }
        }
        //add timeout on fills
        getContext().system().scheduler().scheduleOnce(
                Duration.create(Common.msgTimeoutMs, TimeUnit.MILLISECONDS),  // how frequently generate them
                getSelf(),                                          // destination actor reference
                new FillTimeoutMsg(msg.dataId, msg.requestId),             // the message to send
                getContext().system().dispatcher(),                 // system dispatcher
                getSelf()                                           // source of the message (myself)
        );
    }

    private void onCritWriteRequestMsg(CritWriteRequestMsg msg, ActorRef sender){
        Common.simulateDelay();
        if(sender == null){
            sayError("Received critWriteRequest on dataId: "+msg.dataId+" and value: "+msg.value);
            sender = getSender();
        }
//        say("Received crit write from: "+sender.path().name());
        checkCrashAndUpdate(sender);
        if(checkLocks(msg, sender, true)){
            return;
        }

        if(servedWrites.contains(msg.requestId)){
            say("Request already served, not writing again");
            WriteResponseMsg response = new WriteResponseMsg(msg.dataId, data.get(msg.dataId), msg.requestId);
            sender.tell(response, getSelf());
            return;
        }
        //put a lock on this data until the critical write is not finished
        this.locks.add(msg.dataId);
        this.data.put(msg.dataId, msg.value);
        this.requestsActors.put(msg.requestId, sender);
        //prepare flush message
        FlushRequestMsg flushRequest = new FlushRequestMsg(msg.dataId, msg.requestId);
        HashSet<ActorRef> flushes = new HashSet<>();
        this.critWriteFlushes.put(msg.requestId, flushes);
        for(ActorRef L1 : this.cacheL1){
            if(!crashedCaches.contains(L1)){
                L1.tell(flushRequest, getSelf());
                flushes.add(L1);
            }else{
                //this L1 crashed, need to send the response to its L2
                for(ActorRef L2 : this.cacheL2.get(L1)){
                    if(!crashedCaches.contains(L2)){
                        //send only if not crashed
                        L2.tell(flushRequest, getSelf());
                        flushes.add(L2);
                    }
                }
            }
        }
        //onFlushResponseMsg will remove caches from flushes, so we need to add a timeout here to see if all caches have
        //been removed
        getContext().system().scheduler().scheduleOnce(
                Duration.create(Common.msgTimeoutMs, TimeUnit.MILLISECONDS),  // how frequently generate them
                getSelf(),                                          // destination actor reference
                new FlushTimeoutMsg(msg.requestId, msg.dataId),             // the message to send
                getContext().system().dispatcher(),                 // system dispatcher
                getSelf()                                           // source of the message (myself)
        );
    }

    private void onFlushResponseMsg(FlushResponseMsg msg){
        Common.simulateDelay();
        HashSet<ActorRef> flushes = this.critWriteFlushes.get(msg.requestId);
        flushes.remove(getSender());
        //if all flushes have been received,send refill messages after flush
        if(flushes.isEmpty()){
            this.critWriteFlushes.remove(msg.requestId);
            locks.remove(msg.dataId);
            sendRefillAfterFlush(msg);
        }
    }

    private void onRefillResponseMsg(RefillResponseMsg msg){
        Common.simulateDelay();
        HashSet<ActorRef> fills = this.fillResponses.get(msg.requestId);
        fills.remove(getSender());
//        sayError("Fill confirmation from: "+getSender());
    }

    private void onFlushTimeOutMsg(FlushTimeoutMsg msg){
        Common.simulateDelay();
        HashSet<ActorRef> flushes = this.critWriteFlushes.get(msg.requestId);
        if(flushes == null || flushes.isEmpty()){
            return;
        }
        //check if someone is crashed
        sendChecksForFlush(msg, flushes);
    }

    private void onCheckResponseMsg(CheckResponseMsg msg){
        Common.simulateDelay();
        HashSet<ActorRef> checks = flushChecks.get(msg.requestId);
        checks.remove(getSender());
    }

    private void onCheckTimeoutMsg(CheckTimeoutMsg msg){
        HashSet<ActorRef> checks = flushChecks.get(msg.requestId);
        HashSet<ActorRef> flushes = this.critWriteFlushes.get(msg.requestId);
        if(checks.isEmpty()){
            //no one crashed
            if(flushes == null || flushes.isEmpty()){
                //all flushes obtained
                sendRefillAfterFlush(msg);
                return;
            }
            //no one crashed, but still waiting for a flush confirmation, reschedule the checkmsg
            //re-check if someone is crashed
            sendChecksForFlush(msg, flushes);
        }else{
            //someone crashed, need to contact L2 children or if L2 crashed, set them as crashed
            HashSet<ActorRef> newFlushes = new HashSet<>();
            FlushRequestMsg flushMsg = new FlushRequestMsg(msg.dataId, msg.requestId);
            for(ActorRef crashedCache : checks){
                sayError("Crash detected on: "+crashedCache.path().name());
                flushes.remove(crashedCache);
                if(cacheL2.get(crashedCache)!= null){
                    //crashed cache is l1, contact children
                    ArrayList<ActorRef> l2caches = cacheL2.get(crashedCache);
                    for(ActorRef l2 : l2caches){
                        l2.tell(flushMsg, getSelf());
                        newFlushes.add(l2);
                    }
                }
                //add cache to crashedCaches
                crashedCaches.add(crashedCache);
            }
            if(!newFlushes.isEmpty()){
                //if new flushes have been send, add timeout and wait
                flushes.addAll(newFlushes);
                critWriteFlushes.put(msg.requestId, flushes);
                getContext().system().scheduler().scheduleOnce(
                        Duration.create(Common.msgTimeoutMs, TimeUnit.MILLISECONDS),  // how frequently generate them
                        getSelf(),                                          // destination actor reference
                        new FlushTimeoutMsg(msg.requestId, msg.dataId),             // the message to send
                        getContext().system().dispatcher(),                 // system dispatcher
                        getSelf()                                           // source of the message (myself)
                );
            }else{
                //no new flushes sent, send write messages (with timeout)
                sendRefillAfterFlush(msg);
            }
        }
    }

    private void onFillTimeOutMsg(FillTimeoutMsg msg){
        HashSet<ActorRef> fills = this.fillResponses.get(msg.requestId);
        if(fills == null || fills.isEmpty()){
            return;
        }

        //someone crashed, need to contact L2 children or set L2 as crashed
        RefillRequestMsg refillMsg = new RefillRequestMsg(msg.dataId, data.get(msg.dataId), msg.requestId);
        for(ActorRef crashedCache : fills){
            if(cacheL2.get(crashedCache)!= null){
                //crashed cache is l1, contact children
                ArrayList<ActorRef> l2caches = cacheL2.get(crashedCache);
                for(ActorRef l2 : l2caches){
                    l2.tell(refillMsg, getSelf());
                }
            }
            //add cache to crashedCaches and inform L2 children only if it's added
            boolean firstTimeAdd = crashedCaches.add(crashedCache);
            sayError("Crash detected on: "+crashedCache.path().name());
            if(firstTimeAdd){
                //send a crashedFather msg to all children except sender (cache)
                CrashedFather crashMsg = new CrashedFather();
                for(ActorRef l2: cacheL2.get(crashedCache)){
                    if(l2 != crashedCache){
                        //send message
                        l2.tell(crashMsg, getSelf());
                    }
                }
            }
        }
    }

    private void onCheckConsistencyMsg(CheckConsistencyMsg msg){
//        if(data.get(msg.dataId)!=null){
            say(" dataId: "+msg.dataId + ", value: "+data.get(msg.dataId));
//        }

        consistencyResponses.put(msg.dataId, new ArrayList<ConsistencyCheck>());
        for(ActorRef l1: cacheL1){
            l1.tell(msg, getSelf());
            for(ActorRef l2: cacheL2.get(l1)){
                l2.tell(msg, getSelf());
            }
        }
    }

    /**
     * Check if the sender is a L2 cache. If yes (and the L1 father is not set as crashed) set the L1 father as crashed
     * and inform L2 children that it crashed.
     * @param requestCache
     */
    private void checkCrashAndUpdate(ActorRef requestCache){
        if(l2Fathers.get(requestCache)!=null){
            crashedCaches.remove(requestCache);
            ActorRef crashed = l2Fathers.get(requestCache);
            //message comes from a l2 cache
            if(!crashedCaches.contains(crashed)){
                //if l1 cache was not in crashedCaches, add it and update children
//                sayError("received msg from l2; Crash detected on: "+crashed.path().name());
                crashedCaches.add(crashed);
                //send a crashedFather msg to all children except sender (cache)
                CrashedFather crashMsg = new CrashedFather();
                for(ActorRef l2: cacheL2.get(crashed)){
                    if(l2 != requestCache){
                        //send message
                        l2.tell(crashMsg, getSelf());
                    }
                }
            }
        }
    }

    /**
     * Send refill messages (with timeout) to all L1 caches (and L2 caches whose father crashed). A write response instead of
     * a refill message is sent to the cache that started the request (without timeout).
     * @param msg
     */
    private void sendRefillAfterFlush(Message msg){
        //for the sender of the crit write, send a write response (no timeout needed, if it crashed, the child/client
        //that started the request will resend it)
        WriteResponseMsg response = new WriteResponseMsg(msg.dataId, data.get(msg.dataId), msg.requestId);
        //for all the other caches, send a refill request
        RefillRequestMsg refill = new RefillRequestMsg(msg.dataId, data.get(msg.dataId), msg.requestId);
//        this.critWriteFlushes.remove(msg.requestId);
        this.servedWrites.add(msg.requestId);
        HashSet<ActorRef> fills = new HashSet<>();
        this.fillResponses.put(msg.requestId, fills);
        this.requestsActors.get(msg.requestId).tell(response, getSelf());
        //add fill timeout for L1 only
        for(ActorRef L1 : this.cacheL1){
            if(!crashedCaches.contains(L1)){
                if(L1 != this.requestsActors.get(msg.requestId)) {
                    fills.add(L1);
                    L1.tell(refill, getSelf());
                }
            }else{
                //this L1 crashed, need to send the response to its L2
                for(ActorRef L2 : this.cacheL2.get(L1)){
                    if(!crashedCaches.contains(L2)){
                        //send only if not crashed
                        L2.tell(refill, getSelf());
                        //there is no need for a timeout on L2 (if it crashes, it will lose all the data)
                    }
                }
            }
        }

        //add timeout on caches that refill
        getContext().system().scheduler().scheduleOnce(
                Duration.create(Common.msgTimeoutMs, TimeUnit.MILLISECONDS),  // how frequently generate them
                getSelf(),                                          // destination actor reference
                new FillTimeoutMsg(msg.dataId, msg.requestId),             // the message to send
                getContext().system().dispatcher(),                 // system dispatcher
                getSelf()                                           // source of the message (myself)
        );

        //check if there is a request in the queue and in case process it
        processPendingRequests();

    }

    private void sendChecksForFlush(Message msg, HashSet<ActorRef> flushes){
        HashSet<ActorRef> checks = new HashSet<>();
        flushChecks.put(msg.requestId, checks);
        for(ActorRef cache:flushes){
            CheckMsg checkMsg = new CheckMsg(msg.dataId, msg.requestId);
            cache.tell(checkMsg, getSelf());
            checks.add(cache);
        }

        getContext().system().scheduler().scheduleOnce(
                Duration.create(Common.checkTimeoutMs, TimeUnit.MILLISECONDS),  // how frequently generate them
                getSelf(),                                          // destination actor reference
                new CheckTimeoutMsg(msg.dataId, msg.requestId, null),             // the message to send
                getContext().system().dispatcher(),                 // system dispatcher
                getSelf()                                           // source of the message (myself)
        );
    }

    private void processPendingRequests(){
        Iterator<Message> i = pendingRequestMessages.iterator();
        while(i.hasNext()){
            Message msg = i.next();
            if(!checkLocks(msg, requestsActors.get(msg.requestId), false)){
                if(msg instanceof ReadRequestMsg || msg instanceof CritReadRequestMsg){
                    onReadRequestMsg(msg, requestsActors.get(msg.requestId));
                }else if(msg instanceof WriteRequestMsg){
                    onWriteRequestMsg((WriteRequestMsg) msg, requestsActors.get(msg.requestId));
                }else if(msg instanceof CritWriteRequestMsg){
                    onCritWriteRequestMsg((CritWriteRequestMsg) msg, requestsActors.get(msg.requestId));
                }
                i.remove();
            }

        }
    }

    private void onReadRequestMsgMatch(Message msg){
        onReadRequestMsg(msg, null);
    }

    private void onWriteRequestMsgMatch(WriteRequestMsg msg){
        onWriteRequestMsg(msg, null);
    }

    private void onCritWriteRequestMsgMatch(CritWriteRequestMsg msg){
        onCritWriteRequestMsg(msg, null);
    }

    private void onChildReconnectedMsg(ChildReconnectedMsg msg){
        ActorRef sender = getSender();
        if(cacheL2.get(sender) == null){
            //it's a l2 cache, mark as not crashed
            boolean removed = crashedCaches.remove(sender);
            say("Removed "+sender.path().name()+" from crashedCaches: "+ removed);
            return;
        }
        //if it's a l1 cache, keep it crashed (if already crashed) or set as crashed and inform children
        if(!crashedCaches.contains(getSender())){
            //this is the case when the cache crashes after processing a request: no one other actor knows about
            //the crash, but it lost all the data and inconsistency will arise if L2 children have data
            crashedCaches.add(getSender());
            CrashedFather crashMsg = new CrashedFather();
            for(ActorRef l2: cacheL2.get(getSender())){
                l2.tell(crashMsg, getSelf());
            }
        }

        say("Received reconnect msg from "+sender.path().name());
    }


    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(DatabaseInitializationMsg.class,  this::onInitializationMsg)
                .match(ReadRequestMsg.class, this::onReadRequestMsgMatch)
                .match(WriteRequestMsg.class, this::onWriteRequestMsgMatch)
//                .match(CritReadRequestMsg.class, this::onCritReadRequestMsg)
                .match(CritReadRequestMsg.class, this::onReadRequestMsgMatch)
                .match(CheckConsistencyMsg.class, this::onCheckConsistencyMsg)
                .match(CritWriteRequestMsg.class, this::onCritWriteRequestMsgMatch)
                .match(RefillResponseMsg.class, this::onRefillResponseMsg)
                .match(FlushResponseMsg.class, this::onFlushResponseMsg)
                .match(FlushTimeoutMsg.class, this::onFlushTimeOutMsg)
                .match(FillTimeoutMsg.class, this::onFillTimeOutMsg)
                .match(CheckResponseMsg.class, this::onCheckResponseMsg)
                .match(CheckTimeoutMsg.class, this::onCheckTimeoutMsg)
                .match(ChildReconnectedMsg.class, this::onChildReconnectedMsg)
                .match(CheckConsistencyResponseMsg.class, this::onConsistencyResponse)
                .build();
    }
    private void say(String text){
        System.out.println(getSelf().path().name()+": "+text);
    }
    private void sayError(String text){
        System.err.println(getSelf().path().name()+": "+text);
    }


    private void onConsistencyResponse(CheckConsistencyResponseMsg msg){
//        say("Received from: "+getSender().path().name());
        ArrayList<ConsistencyCheck> list = consistencyResponses.get(msg.dataId);
        list.add(new ConsistencyCheck(getSender(), msg.value));
        if(list.size() == N_CACHES){
            checkConsistency(msg.dataId);
        }
    }

    private void checkConsistency(Integer dataId){
        Integer current = data.get(dataId);
        ArrayList<ConsistencyCheck> list = consistencyResponses.get(dataId);
        int correct = 0;
        ArrayList<ConsistencyCheck> errors = new ArrayList<>();
        for(ConsistencyCheck entry: list){
            if(entry.value==null || entry.value.equals(current)){
                correct++;
            }else{
                errors.add(entry);
            }
        }
        if(correct == N_CACHES){
            sayError("Consistency verified on dataId: "+dataId+" with value "+current);
        }else{
            sayError("Consistency violated on dataId: "+dataId);
//            sayError("Correct value is: "+current);
            sayError("The following caches have wrong values: ");
            for(ConsistencyCheck error: errors){
                sayError(error.cache.path().name()+" ----> "+error.value + " (instead of "+current+")");
            }
        }
    }

    public static class ConsistencyCheck {
        public ActorRef cache;
        public Integer value;

        public ConsistencyCheck(ActorRef cache, Integer value) {
            this.cache = cache;
            this.value = value;
        }
    }
}
