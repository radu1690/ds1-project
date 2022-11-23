package it.unitn.disi.ds1.project;

import akka.actor.ActorRef;
import java.util.concurrent.TimeUnit;
import java.io.Serializable;
import java.util.Random;
import java.util.UUID;

public class Common {

    private static final Random rnd = new Random();
    private static final int delayMs = 10;
    public static final int checkTimeoutMs = 200;
    public static final int msgTimeoutMs = 400;
    public enum CrashType {
        NONE,
        ReadRequest,
        ReadResponse,
        WriteRequest,
        WriteResponse,
        CritReadRequest,
        CritWriteRequest,
        RefillRequest,
        FlushRequest,
        FlushResponse
    }

    public enum CrashTime {
        NONE,
        MessageReceived,
        MessageProcessed,
        Custom
    }

    public static void simulateDelay(){
//        System.out.println("SIMULATING DELAY");
//        System.out.println("BEFORE: "+System.currentTimeMillis());
        try { Thread.sleep(rnd.nextInt(delayMs)); }
        catch (InterruptedException e) { e.printStackTrace(); }
//        System.out.println("AFTER: "+System.currentTimeMillis());
    }

}
