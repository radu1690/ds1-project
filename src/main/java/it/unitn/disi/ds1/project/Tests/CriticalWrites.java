package it.unitn.disi.ds1.project.Tests;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.disi.ds1.project.*;
import it.unitn.disi.ds1.project.Messages.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

/**
 * This file tests the critical write: first 3 clients write on the same dataId 0 to populate some caches. Then a
 * critical write is performed on L2_0_0 --> L1_0 together with 5 concurrent reads. Some read requests will be delayed
 * if they are received during the critical write.
 * You can choose the type of crash and the cache to crash by uncommenting the lines 101-109
 */
public class CriticalWrites {
    final static int N_L1 = 2;
    final static int N_L2 = 1;
    final static int N_CLIENTS = 3;

    ActorRef database;
    List<ActorRef> cacheL1;
    List<ActorRef> cacheL2;
    ArrayList<ActorRef> clients;
    final static ActorSystem system = ActorSystem.create("caches");
    public CriticalWrites(){
        initialize();
        inputContinue();

        testCriticalWrite();

    }

    void initialize(){
        // Create database
        database = system.actorOf(Database.props(), "database");
        //all L1 caches
        cacheL1 = new ArrayList<>();
        //pairs of L1 <--> list of L2
        HashMap<ActorRef, ArrayList<ActorRef>> L2forL1 = new HashMap<>();
        //given a L2, get the father
        HashMap<ActorRef, ActorRef> l2Fathers = new HashMap<>();
        //all clients
        clients = new ArrayList<>();
        //all L2 caches
        cacheL2 = new ArrayList<>();

        //init caches
        for (int i=0; i<N_L1; i++) {
            ArrayList<ActorRef> listL2 = new ArrayList<>();
            //name is: l1_id
            ActorRef l1 = system.actorOf(L1.props(database), "l1_" + i);
            cacheL1.add(l1);

            for(int j = 0; j<N_L2; j++){
                //name is l2_father_id
                ActorRef l2 = system.actorOf(L2.props(database, l1),"l2_" + i + "_" + j );
                listL2.add(l2);
                cacheL2.add(l2);
                l2Fathers.put(l2, l1);
            }
            L2forL1.put(l1, listL2);

            //send l2 list to L1
            L1.L1InitializationMsg init = new L1.L1InitializationMsg(listL2);
            l1.tell(init, ActorRef.noSender());
        }
        //send cacheL1 and cacheL2 to database
        Database.DatabaseInitializationMsg init = new Database.DatabaseInitializationMsg(cacheL1, L2forL1, l2Fathers);
        database.tell(init, ActorRef.noSender());

        //init clients
        for (int i=0; i<N_CLIENTS; i++) {
            ActorRef client = system.actorOf(Client.props(), "client"+i);
            Client.ClientInitializationMsg clientInit = new Client.ClientInitializationMsg(cacheL2);
            client.tell(clientInit, ActorRef.noSender());
            clients.add(client);
        }

        System.out.println("INITIALIZED");
    }

    void testCriticalWrite(){
        StartCritWriteRequestMsg wc1 = new StartCritWriteRequestMsg(0, 1690, cacheL2.get(0));


//        StartWriteRequestMsg w1 = new StartWriteRequestMsg(0, 1, cacheL2.get(9));
//        StartWriteRequestMsg w2 = new StartWriteRequestMsg(0, 2, cacheL2.get(3));
//        StartWriteRequestMsg w3 = new StartWriteRequestMsg(0, 3, cacheL2.get(6));
//
//        StartReadRequestMsg r1 = new StartReadRequestMsg(0, cacheL2.get(9));
//        StartReadRequestMsg r2 = new StartReadRequestMsg(0, cacheL2.get(12));
//        StartReadRequestMsg r3 = new StartReadRequestMsg(0, cacheL2.get(15));
//        StartReadRequestMsg r4 = new StartReadRequestMsg(0, cacheL2.get(17));
//        StartReadRequestMsg r5 = new StartReadRequestMsg(0, cacheL2.get(8));


        //uncomment which crash type and which cache you want to test:
        CrashMsg cr1 = new CrashMsg(Common.CrashType.FlushRequest, Common.CrashTime.MessageReceived);
//        CrashMsg cr1 = new CrashMsg(Common.CrashType.CritWriteRequest, Common.CrashTime.MessageProcessed);
//        CrashMsg cr1 = new CrashMsg(Common.CrashType.FlushRequest, Common.CrashTime.MessageReceived);
//        CrashMsg cr1 = new CrashMsg(Common.CrashType.FlushRequest, Common.CrashTime.MessageProcessed);
//        CrashMsg cr1 = new CrashMsg(Common.CrashType.WriteResponse, Common.CrashTime.MessageReceived);
//        CrashMsg cr1 = new CrashMsg(Common.CrashType.WriteResponse, Common.CrashTime.MessageProcessed);

        cacheL1.get(1).tell(cr1, ActorRef.noSender());
//        cacheL2.get(0).tell(cr1, ActorRef.noSender());



//        clients.get(0).tell(w1, ActorRef.noSender());
//        clients.get(1).tell(w2, ActorRef.noSender());
//        clients.get(2).tell(w3, ActorRef.noSender());

        inputContinue();

        System.out.println("Consistency before critical write:");
        checkEventualConsistency(0);
        inputContinue();
        clients.get(0).tell(wc1, ActorRef.noSender());
        StartReadRequestMsg r1 = new StartReadRequestMsg(0, cacheL2.get(1));
        StartReadRequestMsg r2 = new StartReadRequestMsg(0, cacheL2.get(0));
        clients.get(1).tell(r1, ActorRef.noSender());
        clients.get(2).tell(r2, ActorRef.noSender());

//        clients.get(2).tell(wc1, ActorRef.noSender());
//        clients.get(0).tell(r1, ActorRef.noSender());
//        clients.get(1).tell(r2, ActorRef.noSender());
//        clients.get(3).tell(r3, ActorRef.noSender());
//        clients.get(4).tell(r4, ActorRef.noSender());
//        clients.get(5).tell(r5, ActorRef.noSender());
        inputContinue();


        System.out.println("Consistency after critical write:");
        checkEventualConsistency(0);
        inputContinue();

//        wc1 = new StartCritWriteRequestMsg(0, 9999, cacheL2.get(1));
//        clients.get(3).tell(wc1, ActorRef.noSender());
//        clients.get(0).tell(r1, ActorRef.noSender());
//        clients.get(1).tell(r2, ActorRef.noSender());
//        clients.get(2).tell(r3, ActorRef.noSender());
//        clients.get(4).tell(r4, ActorRef.noSender());
//        clients.get(5).tell(r5, ActorRef.noSender());


        inputContinue();
        checkEventualConsistency(0);
    }

    public static void main(String[] args){
        CriticalWrites m = new CriticalWrites();

        inputContinue();
        system.terminate();
    }


    public static void inputContinue() {
        try {
            System.out.println(">>> Press ENTER to continue <<<");
            System.in.read();
            System.out.println("\n\n");
        }
        catch (IOException ignored) {}
    }

    private void checkEventualConsistency(Integer dataId){
        System.out.println("CHECKING EVENTUAL CONSISTENCY");
        CheckConsistencyMsg msg = new CheckConsistencyMsg(dataId);
        database.tell(msg, ActorRef.noSender());
    }
}
