package it.unitn.disi.ds1.project;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import it.unitn.disi.ds1.project.Messages.*;

public class Main {
    final static int N_L1 = 3;
    final static int N_L2 = 3;
    final static int N_CLIENTS = 6;

    ActorRef database;
    List<ActorRef> cacheL1;
    List<ActorRef> cacheL2;
    ArrayList<ActorRef> clients;
    final static ActorSystem system = ActorSystem.create("caches");
    public Main(){
        initialize();
        inputContinue();

//        testCriticalWrite();
//        testCritReadCrash();
//        testConcurrentWrites();

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

    void testCritReadCrash(){
        StartWriteRequestMsg w1 = new StartWriteRequestMsg(0, 1, cacheL2.get(0));
        StartWriteRequestMsg w2 = new StartWriteRequestMsg(0, 2, cacheL2.get(1));
        StartWriteRequestMsg w3 = new StartWriteRequestMsg(0, 3, cacheL2.get(2));
        clients.get(0).tell(w1, ActorRef.noSender());
        clients.get(1).tell(w2, ActorRef.noSender());
        clients.get(2).tell(w3, ActorRef.noSender());
        CrashMsg cr1 = new CrashMsg(Common.CrashType.ReadResponse, Common.CrashTime.MessageReceived);
        cacheL1.get(0).tell(cr1, ActorRef.noSender());
        System.out.println("Writes and crash sent");
        inputContinue();
        StartWriteRequestMsg w4 = new StartWriteRequestMsg(0, 4, cacheL2.get(4));
        StartCritReadRequestMsg R1 = new StartCritReadRequestMsg(0, cacheL2.get(2));
        clients.get(5).tell(w4, ActorRef.noSender());
        clients.get(4).tell(R1, ActorRef.noSender());
        System.out.println("Crit read sent");
        inputContinue();

        checkEventualConsistency(0);
        inputContinue();

    }

    void testConcurrentWrites(){
        Random rand = new Random(System.currentTimeMillis());
        for(int i = 0; i<5; i++){
            for (ActorRef client : clients) {
                StartWriteRequestMsg w1 = new StartWriteRequestMsg(0, rand.nextInt(100));
                client.tell(w1, ActorRef.noSender());
            }
        }

        inputContinue();

        checkEventualConsistency(0);
        inputContinue();
    }

    void testCriticalWrite(){
        System.out.println("TELL "+cacheL2.get(5).path().name()+" TO CRITICAL WRITE VALUE 1690 IN ID 0");
        StartCritWriteRequestMsg wc1 = new StartCritWriteRequestMsg(0, 1690, cacheL2.get(5));
        StartCritWriteRequestMsg wc2 = new StartCritWriteRequestMsg(0, 1337, cacheL2.get(0));
        StartCritWriteRequestMsg wc3 = new StartCritWriteRequestMsg(0, 420, cacheL2.get(2));
        StartCritWriteRequestMsg wc4 = new StartCritWriteRequestMsg(0, 69, cacheL2.get(4));
        StartCritWriteRequestMsg wc5 = new StartCritWriteRequestMsg(0, 777, cacheL2.get(5));
        StartWriteRequestMsg w1 = new StartWriteRequestMsg(0, 1);
        StartWriteRequestMsg w2 = new StartWriteRequestMsg(0, 2);
        StartWriteRequestMsg w3 = new StartWriteRequestMsg(0, 3);
        StartWriteRequestMsg w4 = new StartWriteRequestMsg(0, 4);
        StartReadRequestMsg r1 = new StartReadRequestMsg(0);
        StartReadRequestMsg r2 = new StartReadRequestMsg(0);
        StartReadRequestMsg r3 = new StartReadRequestMsg(0);
        StartReadRequestMsg r4 = new StartReadRequestMsg(0);
        CrashMsg cr1 = new CrashMsg(Common.CrashType.CritWriteRequest, Common.CrashTime.MessageReceived);
        cacheL2.get(2).tell(cr1, ActorRef.noSender());

        clients.get(0).tell(w1, ActorRef.noSender());
        clients.get(0).tell(w2, ActorRef.noSender());
        clients.get(0).tell(w3, ActorRef.noSender());
        clients.get(0).tell(w4, ActorRef.noSender());

        inputContinue();


        clients.get(2).tell(wc1, ActorRef.noSender());
        clients.get(1).tell(r1, ActorRef.noSender());
        clients.get(1).tell(r2, ActorRef.noSender());
        clients.get(1).tell(r3, ActorRef.noSender());
        clients.get(1).tell(r4, ActorRef.noSender());
        clients.get(2).tell(r1, ActorRef.noSender());
        clients.get(2).tell(r2, ActorRef.noSender());
        clients.get(2).tell(r3, ActorRef.noSender());
        clients.get(2).tell(r4, ActorRef.noSender());
        clients.get(0).tell(r1, ActorRef.noSender());
        clients.get(0).tell(r2, ActorRef.noSender());
        clients.get(0).tell(r3, ActorRef.noSender());
        clients.get(0).tell(r4, ActorRef.noSender());


        clients.get(1).tell(wc2, ActorRef.noSender());
        clients.get(2).tell(wc3, ActorRef.noSender());


        w1 = new StartWriteRequestMsg(0, 11, cacheL2.get(6));
        w2 = new StartWriteRequestMsg(0, 22, cacheL2.get(1));
        w3 = new StartWriteRequestMsg(0, 33, cacheL2.get(2));
        w4 = new StartWriteRequestMsg(0, 44, cacheL2.get(3));
        clients.get(1).tell(w1, ActorRef.noSender());
        clients.get(2).tell(w2, ActorRef.noSender());
        clients.get(3).tell(w3, ActorRef.noSender());
        clients.get(4).tell(w4, ActorRef.noSender());

        w1 = new StartWriteRequestMsg(0, 111, cacheL2.get(6));
        w2 = new StartWriteRequestMsg(0, 222, cacheL2.get(1));
        w3 = new StartWriteRequestMsg(0, 333, cacheL2.get(2));
        w4 = new StartWriteRequestMsg(0, 444, cacheL2.get(3));

        clients.get(2).tell(w1, ActorRef.noSender());
        clients.get(2).tell(w2, ActorRef.noSender());
        clients.get(2).tell(w3, ActorRef.noSender());
        clients.get(2).tell(w4, ActorRef.noSender());


        clients.get(4).tell(wc4, ActorRef.noSender());
        clients.get(0).tell(wc5, ActorRef.noSender());

        clients.get(1).tell(r1, ActorRef.noSender());
        clients.get(1).tell(r2, ActorRef.noSender());
        clients.get(1).tell(r3, ActorRef.noSender());
        clients.get(1).tell(r4, ActorRef.noSender());
        clients.get(2).tell(r1, ActorRef.noSender());
        clients.get(2).tell(r2, ActorRef.noSender());
        clients.get(2).tell(r3, ActorRef.noSender());


        w1 = new StartWriteRequestMsg(0, 1111, cacheL2.get(6));
        w2 = new StartWriteRequestMsg(0, 2222, cacheL2.get(1));
        w3 = new StartWriteRequestMsg(0, 3333, cacheL2.get(2));
        w4 = new StartWriteRequestMsg(0, 4444, cacheL2.get(3));

        clients.get(3).tell(w1, ActorRef.noSender());
        clients.get(2).tell(w2, ActorRef.noSender());
        clients.get(1).tell(w3, ActorRef.noSender());
        clients.get(4).tell(w4, ActorRef.noSender());

        clients.get(2).tell(r4, ActorRef.noSender());
        clients.get(0).tell(r1, ActorRef.noSender());
        clients.get(0).tell(r2, ActorRef.noSender());
        clients.get(0).tell(r3, ActorRef.noSender());
        clients.get(0).tell(r4, ActorRef.noSender());

        inputContinue();
        checkEventualConsistency(0);
    }

    public static void main(String[] args){
        Main m = new Main();

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
//        for(ActorRef cache: cacheL1){
//            cache.tell(msg, ActorRef.noSender());
//        }
//        for(ActorRef cache: cacheL2){
//            cache.tell(msg, ActorRef.noSender());
//        }
    }
}
