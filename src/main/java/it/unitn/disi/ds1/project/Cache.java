package it.unitn.disi.ds1.project;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
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

    //in case of critical write, add lock to a data
//    protected HashSet<Integer> locks;
    //store reads if a critical write is in progress
    protected ArrayList<Messages.Message> pendingReads;
    //type of crash (read/write/flush/etc)
    protected Messages.CrashType nextCrash;
    //when to crash (when receiving a request / after processing it / custom if needed)
    protected Messages.CrashTime nextCrashWhen;
    //keep track of the id of write requests in order to not write a value two times
    protected Set<String> servedWrites;
    //keep track of data that is currently under a critical write and don't read from it
    protected HashMap <String, Boolean> locks;

    public Cache(ActorRef database) {
        this.database = database;
        data = new HashMap<>();
        requestsActors = new HashMap<>();

//        locks = new HashSet<>();
        pendingReads = new ArrayList<>();
        nextCrash = Messages.CrashType.NONE;
        nextCrashWhen = Messages.CrashTime.MessageReceived;
        servedWrites = new HashSet<>();
        locks = new HashMap<>();
    }

//    protected boolean checkLocks(Messages.Message msg, ActorRef sender){
//        if(this.locks.contains(msg.dataId)){
//            System.out.println(getSelf().path().name()+" lock detected on dataId "+ msg.dataId);
//            this.pendingRequestMessages.add(msg);
//            this.requestsActors.put(msg.requestId, sender);
//            System.out.println(getSelf().path().name()+" added msg to pendingRequestMessages");
//            return true;
//        }
//        return false;
//    }


    protected void onCrashMsg(Messages.CrashMsg msg){
        System.out.println(getSelf().path().name()+" received crashMsg");
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
        System.out.println(getSelf().path().name()+" going into crash state");
        getContext().become(crashed());

        getContext().system().scheduler().scheduleOnce(
                Duration.create(recoverySeconds, TimeUnit.SECONDS),  // how frequently generate them
                getSelf(),                                          // destination actor reference
                new Messages.RecoveryMsg(),             // the message to send
                getContext().system().dispatcher(),                 // system dispatcher
                getSelf()                                           // source of the message (myself)
        );
    }

    protected void setLock(Messages.Message msg){
        this.locks.put(msg.requestId, true);
    }

    protected void removeLock(Messages.Message msg){
        this.locks.put(msg.requestId, false);
    }

    protected boolean isLocked(Messages.Message msg){
        return locks.get(msg.requestId) != null && locks.get(msg.requestId);
    }

    protected boolean gonnaCrash(Messages.CrashType type, Messages.CrashTime time){
        if(this.nextCrash == type && this.nextCrashWhen == time){
            this.crash();
            return true;
        }
        return false;
    }

    protected void onRecoveryMsg(Messages.RecoveryMsg msg){
    }

    protected void onCheckConsistencyMsg(Messages.CheckConsistencyMsg msg){
//        if(data.get(msg.dataId)!=null){
            System.out.println(getSelf().path().name()+ " dataId: "+msg.dataId + ", value: "+data.get(msg.dataId));
//        }
    }

    protected void onReadRequestMsg(Messages.ReadRequestMsg m, ActorRef sender){}

    protected void processReads(){
        if(pendingReads.isEmpty()){
            return;
        }
        for(Messages.Message m: pendingReads){
            pendingReads.remove(m);
            if(m instanceof Messages.ReadRequestMsg){
                onReadRequestMsg((Messages.ReadRequestMsg) m, requestsActors.get(m.requestId));
            }else{
                System.err.println("WRONG READ MSG???");
                System.exit(1);
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
