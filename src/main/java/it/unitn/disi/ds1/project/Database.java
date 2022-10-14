package it.unitn.disi.ds1.project;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
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
     * some data is locked (can't be written) if a critical write on it has not yet been finished
     */
    private HashSet<Integer> locks;
    /**
     * store the write requests on locked data in order to process it later on
     */
    private ArrayList<Messages.Message> pendingRequestMessages;

    public Database() {
        this.data = new HashMap<>();
        //this.data.put(0, 55);
        crashedCaches = new HashSet<>();
        servedWrites = new HashSet<>();
        critWriteFlushes = new HashMap<>();
        fillResponses = new HashMap<>();
        requestsActors = new HashMap<>();
        locks = new HashSet<>();
        pendingRequestMessages = new ArrayList<>();
        flushChecks = new HashMap<>();
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

    public static class FlushTimeoutMsg extends Messages.Message {
        public FlushTimeoutMsg(String requestId, Integer dataId) {
            super(dataId, requestId);
        }
    }
    public static class FillTimeoutMsg extends Messages.Message {
        public FillTimeoutMsg(Integer dataId, String requestId) {
            super(dataId, requestId);
        }
    }

    private void onInitializationMsg(DatabaseInitializationMsg msg) {
        this.cacheL1 = msg.listL1;
        this.cacheL2 = msg.hashMapL2;
        this.l2Fathers = msg.l2Fathers;
//        System.err.println("SIZE: "+locks.size());
    }

    /**
     * Check if the requested data has a lock on it. If yes and addToPending is set to true: save the request in pendingRequestsMessages and save also
     * the actor.
     * @param msg
     * @param sender
     * @return true if the data has a lock, false otherwise.
     */
    public boolean checkLocks(Messages.Message msg, ActorRef sender, boolean addToPending){
        if(locks.contains(msg.dataId)){
//            System.err.println("SIZE: "+locks.size());
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

    //this is the same for both normal read and critical read
    private void onReadRequestMsg(Messages.Message msg, ActorRef sender) {
        Messages.simulateDelay();
        if(sender == null){
            sender = getSender();
//            System.out.println(getSelf().path().name()+ ": received read request dataId: "+msg.dataId+" and requestId: "+msg.requestId);
        }

        checkCrashAndUpdate(sender);

        //todo no sure if it's ok to read it if there is a lock
        if(checkLocks(msg, sender, true)){
            return;
        }

        Integer value = this.data.get(msg.dataId);
        //send response with this data
        //no need for timeout
        Messages.ReadResponseMsg response = new Messages.ReadResponseMsg(msg.dataId, value, msg.requestId);
        sender.tell(response, getSelf());
    }


    private void onWriteRequestMsg(Messages.WriteRequestMsg msg, ActorRef sender) {
        Messages.simulateDelay();
        if(sender == null){
            sender = getSender();
        }
        checkCrashAndUpdate(sender);
        if(checkLocks(msg, sender, true)){
            return;
        }

        //if the write request has already been served, no need to write again and update the other caches, simply send
        //back confirmation with the current value
        //this is the case when L1 or L2 crashes when sending confirmation

        if(servedWrites.contains(msg.requestId)){
            say("Request already served, not writing again");
            Messages.WriteResponseMsg response = new Messages.WriteResponseMsg(msg.dataId, data.get(msg.dataId), msg.requestId, false);
            sender.tell(response, getSelf());
            return;
        }
        this.data.put(msg.dataId, msg.value);
        servedWrites.add(msg.requestId);
        Messages.WriteResponseMsg response = new Messages.WriteResponseMsg(msg.dataId, data.get(msg.dataId), msg.requestId, false);
        //send back to sender a writeResponseMsg
        sender.tell(response, getSelf());
        //for all the other caches, send a refillRequestMsg
        Messages.RefillRequestMsg refillRequestMsg = new Messages.RefillRequestMsg(msg.dataId, msg.value, msg.requestId);
        HashSet<ActorRef> fills = new HashSet<>();
        this.fillResponses.put(msg.requestId, fills);
        for(ActorRef L1 : this.cacheL1){
            //send to all L1 (except sender) that did not crash
            if(!crashedCaches.contains(L1)){
                if(L1 != sender){
                    fills.add(L1);
                    L1.tell(refillRequestMsg, getSelf());
                }
            }else{
                //this L1 crashed, need to send the response to its L2
                for(ActorRef L2 : this.cacheL2.get(L1)){
                    if(!crashedCaches.contains(L2)){
                        //send only if not crashed
                        L2.tell(refillRequestMsg, getSelf());
                    }
                }
            }
        }
    }

//    private void onCritReadRequestMsg(Messages.CritReadRequestMsg msg) {
//        checkCrashAndUpdate(getSender());
//        if(checkLocks(msg, getSender())){
//            return;
//        }
//        Integer value = this.data.get(msg.dataId);
////        System.out.println(data);
//        //send response with this data
//
//        Messages.ReadResponseMsg response = new Messages.ReadResponseMsg(msg.dataId, value, msg.requestId);
//        getSender().tell(response, getSelf());
//    }

    private void onCritWriteRequestMsg(Messages.CritWriteRequestMsg msg, ActorRef sender){
        Messages.simulateDelay();
//        System.err.println("RECEIVED CRIT WRITE   "+getSender().path().name());
        if(sender == null){
            sender = getSender();
        }
        checkCrashAndUpdate(sender);
        if(checkLocks(msg, sender, true)){
//            System.err.println("ADDED CRIT WRITE TO PENDING");
            return;
        }

        if(servedWrites.contains(msg.requestId)){
            Messages.WriteResponseMsg response = new Messages.WriteResponseMsg(msg.dataId, data.get(msg.dataId), msg.requestId, false);

            sender.tell(response, getSelf());

            return;
        }
        //put a lock on this data until the crit write is not finished
        this.locks.add(msg.dataId);
        this.data.put(msg.dataId, msg.value);
        this.requestsActors.put(msg.requestId, sender);
        Messages.FlushRequestMsg flushRequest = new Messages.FlushRequestMsg(msg.dataId, msg.requestId);
        HashSet<ActorRef> flushes = new HashSet<>();
        this.critWriteFlushes.put(msg.requestId, flushes);
//        System.out.println("CRITWRITE: "+ msg.requestId);
        for(ActorRef L1 : this.cacheL1){
            if(!crashedCaches.contains(L1)){
                L1.tell(flushRequest, getSelf());
                flushes.add(L1);
//                System.out.println(L1);
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
            //System.out.println(flushes);
        }
//        this.critWriteFlushes.put(msg.requestId, flushes);
        //onFlushResponseMsg will remove caches from flushes, so we need to add a timeout here to see if all caches have
        //been removed
        //COMPLETED add flush timeout
        getContext().system().scheduler().scheduleOnce(
                Duration.create(400, TimeUnit.MILLISECONDS),  // how frequently generate them
                getSelf(),                                          // destination actor reference
                new FlushTimeoutMsg(msg.requestId, msg.dataId),             // the message to send
                getContext().system().dispatcher(),                 // system dispatcher
                getSelf()                                           // source of the message (myself)
        );
    }

    private void onFlushResponseMsg(Messages.FlushResponseMsg msg){
        Messages.simulateDelay();
        HashSet<ActorRef> flushes = this.critWriteFlushes.get(msg.requestId);
        flushes.remove(getSender());


//        System.err.println(getSender().path().name()+"   "+ msg);


        //if all flushes have been received,send refill msgs after flush
        if(flushes.isEmpty()){
//            say("All flushes received!");
//            System.err.println(getSender().path().name());
//            System.err.println(msg);
            this.critWriteFlushes.remove(msg.requestId);
            locks.remove(msg.dataId);
            sendRefillAfterFlush(msg);
        }

    }

    private void onRefillResponseMsg(Messages.RefillResponseMsg msg){
        Messages.simulateDelay();
        HashSet<ActorRef> fills = this.fillResponses.get(msg.requestId);
        fills.remove(getSender());
    }

    private void onFlushTimeOutMsg(FlushTimeoutMsg msg){
        Messages.simulateDelay();
        HashSet<ActorRef> flushes = this.critWriteFlushes.get(msg.requestId);
        if(flushes == null || flushes.isEmpty()){
            return;
        }

        //check if someone is crashed
        HashSet<ActorRef> checks = new HashSet<>();
        flushChecks.put(msg.requestId, checks);
        for(ActorRef cache:flushes){
            Messages.CheckMsg checkMsg = new Messages.CheckMsg(msg.dataId, msg.requestId);
            cache.tell(checkMsg, getSelf());
            checks.add(cache);
        }
        //COMPLETED add check timeout
        getContext().system().scheduler().scheduleOnce(
                Duration.create(100, TimeUnit.MILLISECONDS),  // how frequently generate them
                getSelf(),                                          // destination actor reference
                new Messages.CheckTimeoutMsg(msg.dataId, msg.requestId, null),             // the message to send
                getContext().system().dispatcher(),                 // system dispatcher
                getSelf()                                           // source of the message (myself)
        );
        //--------------------


    }

    private void onCheckResponseMsg(Messages.CheckResponseMsg msg){
        //Messages.simulateDelay();
        HashSet<ActorRef> checks = flushChecks.get(msg.requestId);
        checks.remove(getSender());
    }

    private void onCheckTimeoutMsg(Messages.CheckTimeoutMsg msg){
        HashSet<ActorRef> checks = flushChecks.get(msg.requestId);
        if(checks.isEmpty()){
            //no one crashed
            HashSet<ActorRef> flushes = this.critWriteFlushes.get(msg.requestId);
            if(flushes == null || flushes.isEmpty()){
                //all flushes obtained
                sendRefillAfterFlush(msg);
                return;
            }

            //no one crashed, but still waiting for a flush confirmation, reschedule the checkmsg
            //re-check if someone is crashed
            HashSet<ActorRef> newChecks = new HashSet<>();
            flushChecks.put(msg.requestId, newChecks);
            for(ActorRef cache:flushes){
                Messages.CheckMsg checkMsg = new Messages.CheckMsg(msg.dataId, msg.requestId);
                cache.tell(checkMsg, getSelf());
                checks.add(cache);
            }

            getContext().system().scheduler().scheduleOnce(
                    Duration.create(100, TimeUnit.MILLISECONDS),  // how frequently generate them
                    getSelf(),                                          // destination actor reference
                    new Messages.CheckTimeoutMsg(msg.dataId, msg.requestId, null),             // the message to send
                    getContext().system().dispatcher(),                 // system dispatcher
                    getSelf()                                           // source of the message (myself)
            );
        }else{
            //someone crashed, need to contact L2 children or if L2 crashed, set them as crashed
//            System.err.println("DB: Someone crashed");
            HashSet<ActorRef> flushes = this.critWriteFlushes.get(msg.requestId);
            HashSet<ActorRef> newFlushes = new HashSet<>();
            Messages.FlushRequestMsg flushMsg = new Messages.FlushRequestMsg(msg.dataId, msg.requestId);
            for(ActorRef crashedCache : flushes){
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
                critWriteFlushes.put(msg.requestId, newFlushes);
                getContext().system().scheduler().scheduleOnce(
                        Duration.create(400, TimeUnit.MILLISECONDS),  // how frequently generate them
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
        Messages.RefillRequestMsg refillMsg = new Messages.RefillRequestMsg(msg.dataId, data.get(msg.dataId), msg.requestId);
        for(ActorRef crashedCache : fills){
            if(cacheL2.get(crashedCache)!= null){
                //crashed cache is l1, contact children
                ArrayList<ActorRef> l2caches = cacheL2.get(crashedCache);
                for(ActorRef l2 : l2caches){
                    l2.tell(refillMsg, getSelf());
                }
            }
            //add cache to crashedCaches
            crashedCaches.add(crashedCache);
        }
    }

    private void onCheckConsistencyMsg(Messages.CheckConsistencyMsg msg){
//        if(data.get(msg.dataId)!=null){
            System.out.println(getSelf().path().name()+ " dataId: "+msg.dataId + ", value: "+data.get(msg.dataId));
//        }
    }

    /**
     * Check if the sender is a L2 cache. If yes (and the L1 father is not set as crashed) set the L1 father as crashed
     * and inform L2 children that it crashed.
     * @param requestCache
     */
    private void checkCrashAndUpdate(ActorRef requestCache){
        if(l2Fathers.get(requestCache)!=null){
            ActorRef crashed = l2Fathers.get(requestCache);
            //message comes from a l2 cache
            if(!crashedCaches.contains(crashed)){
                //if l1 cache was not in crashedCaches, add it and update children
                crashedCaches.add(crashed);
                //send a crashedFather msg to all children except sender (cache)
                Messages.CrashedFather crashMsg = new Messages.CrashedFather();
                for(ActorRef l2: cacheL2.get(crashed)){
                    if(l2 != requestCache){
                        //send message
                        l2.tell(crashMsg, getSelf());
                    }
                }
            }
        }
    }

    private void sendRefillAfterFlush(Messages.Message msg){
        //for the sender of the crit write, send a write response (no timeout needed, if it crashed, the child/client
        //that started the request will resend it)
        Messages.WriteResponseMsg response = new Messages.WriteResponseMsg(msg.dataId, data.get(msg.dataId), msg.requestId, true);
        //for all the other caches, send a refill request
        Messages.RefillRequestMsg refill = new Messages.RefillRequestMsg(msg.dataId, data.get(msg.dataId), msg.requestId);
//        this.critWriteFlushes.remove(msg.requestId);
        this.servedWrites.add(msg.requestId);
        HashSet<ActorRef> fills = new HashSet<>();
        this.fillResponses.put(msg.requestId, fills);
        this.requestsActors.get(msg.requestId).tell(response, getSelf());
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
                        L2.tell(response, getSelf());
                        //there is no need for a timeout on L2 (if it crashes, it will lose all the data)
                    }
                }
            }
        }

        //add timeout on caches that refill
        getContext().system().scheduler().scheduleOnce(
                Duration.create(400, TimeUnit.MILLISECONDS),  // how frequently generate them
                getSelf(),                                          // destination actor reference
                new FillTimeoutMsg(msg.dataId, msg.requestId),             // the message to send
                getContext().system().dispatcher(),                 // system dispatcher
                getSelf()                                           // source of the message (myself)
        );

        //check if there is a request in the queue and in case process it
        processPendingRequests();

    }

    private void processPendingRequests(){
        Iterator<Messages.Message> i = pendingRequestMessages.iterator();
        while(i.hasNext()){
            Messages.Message msg = i.next();
            if(!checkLocks(msg, requestsActors.get(msg.requestId), false)){
                if(msg instanceof Messages.ReadRequestMsg || msg instanceof Messages.CritReadRequestMsg){
                    onReadRequestMsg(msg, requestsActors.get(msg.requestId));
                }else if(msg instanceof Messages.WriteRequestMsg){
                    onWriteRequestMsg((Messages.WriteRequestMsg) msg, requestsActors.get(msg.requestId));
                }else if(msg instanceof Messages.CritWriteRequestMsg){
                    onCritWriteRequestMsg((Messages.CritWriteRequestMsg) msg, requestsActors.get(msg.requestId));
                    //at the end of a critical write, this method will be called again
//                System.err.println("BREAK!!!!");
////                break;
                }
                i.remove();
//                System.out.println("Pending size: "+pendingRequestMessages.size());
            }

        }



//        for(Messages.Message msg: pendingRequestMessages){
//            say("Process pending request ("+pendingRequestMessages.size()+")");
////            pendingRequestMessages.remove(msg);
//            if(msg instanceof Messages.ReadRequestMsg || msg instanceof Messages.CritReadRequestMsg){
//                onReadRequestMsg(msg, requestsActors.get(msg.requestId));
//            }else if(msg instanceof Messages.WriteRequestMsg){
//                onWriteRequestMsg((Messages.WriteRequestMsg) msg, requestsActors.get(msg.requestId));
//            }else if(msg instanceof Messages.CritWriteRequestMsg){
//                onCritWriteRequestMsg((Messages.CritWriteRequestMsg) msg, requestsActors.get(msg.requestId));
//                //at the end of a critical write, this method will be called again
//                System.err.println("BREAK!!!!");
////                break;
//            }
//        }
    }

    private void onReadRequestMsgMatch(Messages.Message msg){
        onReadRequestMsg(msg, null);
    }

    private void onWriteRequestMsgMatch(Messages.WriteRequestMsg msg){
        onWriteRequestMsg(msg, null);
    }

    private void onCritWriteRequestMsgMatch(Messages.CritWriteRequestMsg msg){
        onCritWriteRequestMsg(msg, null);
    }

    private void onChildReconnectedMsg(Messages.ChildReconnectedMsg msg){
        ActorRef sender = getSender();
        if(cacheL2.get(sender) == null){
            //it's a l2 cache, mark as not crashed
            boolean removed = crashedCaches.remove(sender);
            say("Removed "+sender.path().name()+" from crashedCaches: "+ removed);
            return;
        }
        //if it's a l1 cache, keep it crashed
        say("Received reconnect msg from "+sender.path().name());
    }


    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(DatabaseInitializationMsg.class,  this::onInitializationMsg)
                .match(Messages.ReadRequestMsg.class, this::onReadRequestMsgMatch)
                .match(Messages.WriteRequestMsg.class, this::onWriteRequestMsgMatch)
//                .match(Messages.CritReadRequestMsg.class, this::onCritReadRequestMsg)
                .match(Messages.CritReadRequestMsg.class, this::onReadRequestMsgMatch)
                .match(Messages.CheckConsistencyMsg.class, this::onCheckConsistencyMsg)
                .match(Messages.CritWriteRequestMsg.class, this::onCritWriteRequestMsgMatch)
                .match(Messages.RefillResponseMsg.class, this::onRefillResponseMsg)
                .match(Messages.FlushResponseMsg.class, this::onFlushResponseMsg)
                .match(FlushTimeoutMsg.class, this::onFlushTimeOutMsg)
                .match(FillTimeoutMsg.class, this::onFillTimeOutMsg)
                .match(Messages.CheckResponseMsg.class, this::onCheckResponseMsg)
                .match(Messages.CheckTimeoutMsg.class, this::onCheckTimeoutMsg)
                .match(Messages.ChildReconnectedMsg.class, this::onChildReconnectedMsg)
                .build();
    }
    private void say(String text){
        System.out.println(getSelf().path().name()+": "+text);
    }
    private void sayError(String text){
        System.err.println(getSelf().path().name()+": "+text);
    }
}
