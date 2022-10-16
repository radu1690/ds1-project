package it.unitn.disi.ds1.project;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
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
    protected ArrayList<Messages.Message> pendingReads;
    //type of crash (read/write/flush/etc)
    protected Messages.CrashType nextCrash;
    //when to crash (when receiving a request / after processing it / custom if needed)
    protected Messages.CrashTime nextCrashWhen;
    //keep track of the id of write requests in order to not write a value two times
    protected Set<String> servedWrites;
    //keep track of data that is currently under a critical write and don't read from it
    protected HashMap <Integer, Boolean> locks;

    public Cache(ActorRef database) {
        this.database = database;
        data = new HashMap<>();
        requestsActors = new HashMap<>();
        pendingReads = new ArrayList<>();
        nextCrash = Messages.CrashType.NONE;
        nextCrashWhen = Messages.CrashTime.MessageReceived;
        servedWrites = new HashSet<>();
        locks = new HashMap<>();
    }

    protected void onCrashMsg(Messages.CrashMsg msg){
        say(" received crashMsg");
        nextCrash = msg.nextCrash;
        nextCrashWhen = msg.nextCrashWhen;
    }

    protected void onCheckMsg(Messages.CheckMsg msg){
        say("Received checkMsg from: "+getSender().path().name());
        ActorRef sender = getSender();
        Messages.CheckResponseMsg m = new Messages.CheckResponseMsg(msg.dataId, msg.requestId);
        sender.tell(m, getSelf());
    }

    protected void crash(){
        sayError(" going into crash state");
        getContext().become(crashed());
        //schedule msg to recovery
        getContext().system().scheduler().scheduleOnce(
                Duration.create(recoverySeconds, TimeUnit.SECONDS),  // how frequently generate them
                getSelf(),                                          // destination actor reference
                new Messages.RecoveryMsg(),             // the message to send
                getContext().system().dispatcher(),                 // system dispatcher
                getSelf()                                           // source of the message (myself)
        );
    }

    protected void setLock(Messages.Message msg){
//        Integer current = locks.get(msg.dataId);
//        if(current == null){
//            current = 1;
//        }else{
//            current = current + 1;
//        }
//        correctLock(msg);
//        this.locks.put(msg.dataId, current);
        this.locks.put(msg.dataId, true);
    }

    protected void removeLock(Messages.Message msg){
//        Integer current = locks.get(msg.dataId);
//        if(current == null || current == 0){
//            current = 0;
//        }else{
//            current = current - 1;
//        }
//        correctLock(msg);
//        this.locks.put(msg.dataId, current);
        this.locks.put(msg.dataId, false);
    }

    protected boolean isLocked(Messages.Message msg){
//        Integer current = locks.get(msg.dataId);
//        correctLock(msg);
//        return current != null && current != 0;
        return locks.get(msg.dataId) != null && locks.get(msg.dataId);
    }

    /**
     * Returns true if it goes in crash mode, false otherwise.
     * @param type
     * @param time
     * @return
     */
    protected boolean gonnaCrash(Messages.CrashType type, Messages.CrashTime time){
        if(this.nextCrash == type && this.nextCrashWhen == time){
            this.crash();
            return true;
        }
        return false;
    }
    //different for l1 and l2
    protected void onRecoveryMsg(Messages.RecoveryMsg msg){
    }

    protected void onCheckConsistencyMsg(Messages.CheckConsistencyMsg msg){
//        if(data.get(msg.dataId)!=null){
            System.out.println(getSelf().path().name()+ " dataId: "+msg.dataId + ", value: "+data.get(msg.dataId));
//        }
    }

    protected void onReadRequestMsg(Messages.ReadRequestMsg m, ActorRef sender){}

    //process pending reads
    protected void processReads() {
        if (pendingReads.isEmpty()) {
            return;
        }
        Iterator<Messages.Message> i = pendingReads.iterator();
        while (i.hasNext()) {
            Messages.Message msg = i.next();
            if (!isLocked(msg)) {
                if (msg instanceof Messages.ReadRequestMsg) {
                    onReadRequestMsg((Messages.ReadRequestMsg) msg, requestsActors.get(msg.requestId));
                } else {
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
                .match(Messages.RecoveryMsg.class, this::onRecoveryMsg)
                .matchAny(msg -> {})
                //.matchAny(msg -> System.out.println(getSelf().path().name() + " ignoring " + msg.getClass().getSimpleName() + " (crashed)"))
                .build();
    }
}
