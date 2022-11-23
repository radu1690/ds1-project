package it.unitn.disi.ds1.project;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import it.unitn.disi.ds1.project.Messages.*;
import scala.concurrent.duration.Duration;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class Cache extends AbstractActor {
    //seconds to pass before recovering
    private final int recoverySeconds = 5;
    //actoref of the main database
    protected ActorRef database;
    //local data
    protected HashMap<Integer, Integer> data;
    //for each request, keep track of the actor that sent it in order to respond later on
    protected HashMap<String, ActorRef> requestsActors;

    //store reads if a critical write is in progress
    protected ArrayList<Message> pendingReads;
    //type of crash (read/write/flush/etc)
    protected Common.CrashType nextCrash;
    //when to crash (when receiving a request / after processing it / custom if needed)
    protected Common.CrashTime nextCrashWhen;
    //keep track of the id of write requests in order to not write a value two times
    protected Set<String> servedWrites;
    //keep track of data that is currently under a critical write and don't read from it
//    protected HashMap <Integer, Boolean> locks;
    protected HashSet<Integer> locks;


    public Cache(ActorRef database) {
        this.database = database;
        data = new HashMap<>();
        requestsActors = new HashMap<>();
        pendingReads = new ArrayList<>();
        nextCrash = Common.CrashType.NONE;
        nextCrashWhen = Common.CrashTime.NONE;
        servedWrites = new HashSet<>();
//        locks = new HashMap<>();
        locks = new HashSet<>();
    }

    protected void onCrashMsg(CrashMsg msg){
        say(" received crashMsg");
        nextCrash = msg.nextCrash;
        nextCrashWhen = msg.nextCrashWhen;
    }

    protected void onCheckMsg(CheckMsg msg){
        say("Received checkMsg from: "+getSender().path().name());
        ActorRef sender = getSender();
        CheckResponseMsg m = new CheckResponseMsg(msg.dataId, msg.requestId);
        sender.tell(m, getSelf());
    }

    protected void crash(){
        sayError(" going into crash state");
        data = new HashMap<>();
        getContext().become(crashed());
        //schedule msg to recovery
        getContext().system().scheduler().scheduleOnce(
                Duration.create(recoverySeconds, TimeUnit.SECONDS),  // how frequently generate them
                getSelf(),                                          // destination actor reference
                new RecoveryMsg(),             // the message to send
                getContext().system().dispatcher(),                 // system dispatcher
                getSelf()                                           // source of the message (myself)
        );
    }

    protected void setLock(Message msg){
//        this.locks.put(msg.dataId, true);
        this.locks.add(msg.dataId);
    }

    protected void removeLock(Message msg){
//        this.locks.put(msg.dataId, false);
        this.locks.remove(msg.dataId);
    }

    protected boolean isLocked(Message msg){
//        return locks.get(msg.dataId) != null && locks.get(msg.dataId);
        return locks.contains(msg.dataId);
    }

    /**
     * Returns true if it goes in crash mode, false otherwise.
     * @param type
     * @param time
     * @return
     */
    protected boolean gonnaCrash(Common.CrashType type, Common.CrashTime time){
        if(this.nextCrash == type && this.nextCrashWhen == time){
            this.crash();
            return true;
        }
        return false;
    }
    //different for l1 and l2
    protected void onRecoveryMsg(RecoveryMsg msg){}

    protected void onCheckConsistencyMsg(CheckConsistencyMsg msg){
//        if(data.get(msg.dataId)!=null){
            System.out.println(getSelf().path().name()+ " dataId: "+msg.dataId + ", value: "+data.get(msg.dataId));
//        }
        CheckConsistencyResponseMsg response = new CheckConsistencyResponseMsg(msg.dataId, data.get(msg.dataId));
        getSender().tell(response, getSelf());
    }

    protected void onReadRequestMsg(ReadRequestMsg m, ActorRef sender){}

    //process pending reads
    protected void processReads() {
        if (pendingReads.isEmpty()) {
            return;
        }
        Iterator<Message> i = pendingReads.iterator();
        while (i.hasNext()) {
            Message msg = i.next();
            if (!isLocked(msg)) {
                if (msg instanceof ReadRequestMsg) {
                    onReadRequestMsg((ReadRequestMsg) msg, requestsActors.get(msg.requestId));
                } else {
                    //critical reads are sent to database which will add them to the pending requests
                    System.err.println("WRONG READ MSG???");
                    System.exit(1);
                }
                i.remove();
            }
        }
    }

    protected void say(String text){
        System.out.println(getSelf().path().name()+": "+text);
    }

    protected void sayError(String text){
        System.err.println(getSelf().path().name()+": "+text);
    }

    @Override
    public Receive createReceive() {
        return null;
    }

    final AbstractActor.Receive crashed() {
        return receiveBuilder()
                .match(RecoveryMsg.class, this::onRecoveryMsg)
                .match(CheckConsistencyMsg.class, this::onCheckConsistencyMsg)
                .matchAny(msg -> {})
                //.matchAny(msg -> System.out.println(getSelf().path().name() + " ignoring " + msg.getClass().getSimpleName() + " (crashed)"))
                .build();
    }



}
