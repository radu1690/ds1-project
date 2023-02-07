package it.unitn.disi.ds1.project.Tests;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.disi.ds1.project.*;
import it.unitn.disi.ds1.project.Messages.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class CriticalReads {
    final static int N_L1 = 3;
    final static int N_L2 = 6;
    final static int N_CLIENTS = 40;

    ActorRef database;
    List<ActorRef> cacheL1;
    List<ActorRef> cacheL2;
    ArrayList<ActorRef> clients;
    final static ActorSystem system = ActorSystem.create("caches");
    public CriticalReads(){
        initialize();
        sendCrashMsg();
        inputContinue();

        testReads();

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
        CrashMsg cr1 = new CrashMsg(Common.CrashType.CritReadRequest, Common.CrashTime.MessageReceived);
        CrashMsg cr2 = new CrashMsg(Common.CrashType.CritReadRequest, Common.CrashTime.MessageProcessed);
        CrashMsg cr3 = new CrashMsg(Common.CrashType.CritReadRequest, Common.CrashTime.MessageProcessed);
        //cacheL1 -> 0-2
        //cacheL2 -> 0-17

//        cacheL2.get(0).tell(cr1, ActorRef.noSender());
//        cacheL1.get(1).tell(cr2, ActorRef.noSender());
        cacheL2.get(11).tell(cr3, ActorRef.noSender());
    }

    void testReads(){
        StartWriteRequestMsg w1 = new StartWriteRequestMsg(0, 55, cacheL2.get(0));
        clients.get(0).tell(w1, ActorRef.noSender());
        inputContinue();

        checkEventualConsistency(0);
        inputContinue();

        StartCritReadRequestMsg r1 = new StartCritReadRequestMsg(0, cacheL2.get(0));
        clients.get(1).tell(r1, ActorRef.noSender());
        inputContinue();

        checkEventualConsistency(0);
        inputContinue();

        StartCritReadRequestMsg r2 = new StartCritReadRequestMsg(0, cacheL2.get(1));
        clients.get(2).tell(r2, ActorRef.noSender());
        inputContinue();

        checkEventualConsistency(0);
        inputContinue();

        StartCritReadRequestMsg r3 = new StartCritReadRequestMsg(0, cacheL2.get(11));
        clients.get(3).tell(r3, ActorRef.noSender());
        inputContinue();

        checkEventualConsistency(0);
        inputContinue();
    }


    public static void main(String[] args){
        CriticalReads m = new CriticalReads();

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
