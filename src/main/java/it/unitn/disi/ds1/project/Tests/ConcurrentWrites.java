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
 * In this test 20 clients write random data on dataId 0-1-2 for 5 times (each time consistency is checked).
 * You can choose which cache to crash in sendCrashMsg function.
 */
public class ConcurrentWrites {
    final static int N_L1 = 3;
    final static int N_L2 = 6;
    final static int N_CLIENTS = 20;

    ActorRef database;
    List<ActorRef> cacheL1;
    List<ActorRef> cacheL2;
    ArrayList<ActorRef> clients;
    final static ActorSystem system = ActorSystem.create("caches");
    public ConcurrentWrites(){
        initialize();
        sendCrashMsg();
        inputContinue();

        testConcurrentWrites();

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

    void sendCrashMsg(){
        CrashMsg cr1 = new CrashMsg(Common.CrashType.WriteRequest, Common.CrashTime.MessageProcessed);
        //cacheL1 -> 0-2
        //cacheL2 -> 0-17
        cacheL1.get(2).tell(cr1, ActorRef.noSender());
    }

    void testConcurrentWrites(){
        Random rand = new Random(System.currentTimeMillis());
        for(int i = 0; i<5; i++){
            for (ActorRef client : clients) {
                int dataId = rand.nextInt(3);
                StartWriteRequestMsg w1 = new StartWriteRequestMsg(dataId, rand.nextInt(100));
                client.tell(w1, ActorRef.noSender());
            }
            inputContinue();
            checkEventualConsistency(0);
            checkEventualConsistency(1);
            checkEventualConsistency(2);
            inputContinue();
        }

        inputContinue();
    }


    public static void main(String[] args){
        ConcurrentWrites m = new ConcurrentWrites();

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
