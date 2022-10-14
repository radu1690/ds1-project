package it.unitn.disi.ds1.project;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.Random;
import java.util.UUID;

public class Messages {

    private static final Random rnd = new Random();
    private static final int delayMs = 30;
    public enum CrashType {
        NONE,
        ReadRequest,
        ReadResponse,
        WriteRequest,
        WriteResponse,
        CritReadRequest,
        CritWriteRequest,
        RefillRequest,
        FlushRequest
    }

    public enum CrashTime {
        MessageReceived,
        MessageProcessed,
        Custom
    }

    public static void simulateDelay(){
//        System.out.println("SIMULATING DELAY");
//        System.out.println("BEFORE: "+System.currentTimeMillis());
        try { Thread.sleep(rnd.nextInt(delayMs)); }
//        try { Thread.sleep(1500); }
        catch (InterruptedException e) { e.printStackTrace(); }
//        System.out.println("AFTER: "+System.currentTimeMillis());
    }

    public static class Message implements Serializable {
        public final Integer dataId;
        public final String requestId;
        public Message(Integer dataId, String requestId) {
            this.dataId = dataId;
            this.requestId = requestId;
        }

    }

    public static class ReadRequestMsg extends Message {
        public ReadRequestMsg(Integer dataId) {
            super(dataId, UUID.randomUUID().toString());
        }
    }

    //sent from main to client
    public static class StartReadRequestMsg extends Message {
        public final ActorRef l2;
        public StartReadRequestMsg(Integer dataId, ActorRef l2) {
            super(dataId, null);
            this.l2 = l2;
        }
    }

    public static class ReadResponseMsg extends Message {
        public final Integer value;
        public ReadResponseMsg(Integer dataId, Integer value, String requestId) {
            super(dataId, requestId);
            this.value = value;
        }
    }

    public static class WriteRequestMsg extends Message {
        public final Integer value;
        public WriteRequestMsg(Integer dataId, Integer value) {
            super(dataId, UUID.randomUUID().toString());
            this.value = value;
        }
        public WriteRequestMsg(Integer dataId, Integer value, String requestId){
            super(dataId, requestId);
            this.value = value;
        }
    }
    //sent from main to client
    public static class StartWriteRequestMsg extends Message {
        public final Integer value;
        public final ActorRef l2;
        public StartWriteRequestMsg(Integer dataId, Integer value, ActorRef l2) {
            super(dataId, null);
            this.value = value;
            this.l2 = l2;
        }
    }

    public static class WriteResponseMsg extends Message {
        //current value of the data (might be different if write has already been served)
        public final Integer currentValue;
        //if the request has already been served but the current value is different (another write happened),
        //set valid to false
//        public final Boolean valid;
        public final Boolean afterFlush;
//        public WriteResponseMsg(Integer dataId, Integer value, String requestId, Boolean valid, Boolean afterFlush) {
          public WriteResponseMsg(Integer dataId, Integer value, String requestId, Boolean afterFlush) {
            super(dataId, requestId);
            this.currentValue = value;
//            this.valid = valid;
            this.afterFlush = afterFlush;
        }
    }

    public static class CritReadRequestMsg extends Message {
        public CritReadRequestMsg(Integer dataId) {
            super(dataId, UUID.randomUUID().toString());
        }
    }
    //sent from main to client
    public static class StartCritReadRequestMsg extends Message {
        public final ActorRef l2;
        public StartCritReadRequestMsg(Integer dataId, ActorRef l2) {
            super(dataId, null);
            this.l2 = l2;
        }
    }

    public static class CritWriteRequestMsg extends Message {
        public final Integer value;
        public CritWriteRequestMsg(Integer dataId, Integer value) {
            super(dataId, UUID.randomUUID().toString());
            this.value = value;
        }
        public CritWriteRequestMsg(Integer dataId, Integer value, String requestId){
            super(dataId, requestId);
            this.value = value;
        }
    }
    //sent from main to client
    public static class StartCritWriteRequestMsg extends Message {
        public final Integer value;
        public final ActorRef l2;
        public StartCritWriteRequestMsg(Integer dataId, Integer value, ActorRef l2) {
            super(dataId, null);
            this.value = value;
            this.l2 = l2;
        }
    }
    public static class FlushRequestMsg extends Message {
//        public FlushRequestMsg(Integer dataId) {
//            super(dataId, UUID.randomUUID().toString());
//        }
        public FlushRequestMsg(Integer dataId, String requestId){
            super(dataId, requestId);
        }
    }

    public static class FlushResponseMsg extends Message {
        public FlushResponseMsg(Integer dataId, String requestId) {
            super(dataId, requestId);
        }

        @Override
        public String toString() {
            return "FlushResponseMsg{" +
                    "dataId=" + dataId +
                    ", requestId='" + requestId + '\'' +
                    '}';
        }
    }

    public static class RefillRequestMsg extends Message {
        public final Integer value;
        public RefillRequestMsg(Integer dataId, Integer value, String requestId) {
            super(dataId, requestId);
            this.value = value;
        }
    }

    public static class RefillResponseMsg extends Message {
        public final Integer value;
        public RefillResponseMsg(Integer dataId, Integer value, String requestId) {
            super(dataId, requestId);
            this.value = value;
        }
    }

    public static class CheckConsistencyMsg extends Message {
        public CheckConsistencyMsg(Integer dataId) {
            super(dataId, null);
        }
    }

    //message used to check if an actor (l1 or l2) is still online
    public static class CheckMsg extends Message {
        public CheckMsg(Integer dataId, String requestId) {
            super(dataId, requestId);
        }
    }

    //message used to check if an actor (l1 or l2) is still online
    public static class CheckResponseMsg extends Message {
        public CheckResponseMsg(Integer dataId, String requestId) {
            super(dataId, requestId);
        }
    }

    public static class CheckTimeoutMsg extends Message {
        public final ActorRef receiver;
        public CheckTimeoutMsg(Integer dataId, String requestId, ActorRef receiver){
            super(dataId, requestId);
            this.receiver = receiver;
        }
    }

    public static class SelfTimeoutMsg extends Message {
        public final ActorRef receiver;
        public SelfTimeoutMsg(Integer dataId, String requestId, ActorRef receiver) {
            super(dataId, requestId);
            this.receiver = receiver;
        }
    }

    public static class CrashMsg implements Serializable {
        public final CrashType nextCrash;
        public final CrashTime nextCrashWhen;
        public CrashMsg(CrashType nextCrash, CrashTime nextCrashWhen) {
            this.nextCrash = nextCrash;
            this.nextCrashWhen = nextCrashWhen;
        }
    }
    public static class CrashedFather implements Serializable {
        public CrashedFather() {
        }
    }




    public static class RecoveryMsg implements Serializable{
    }

    public static class ChildReconnectedMsg implements Serializable{
    }

}
