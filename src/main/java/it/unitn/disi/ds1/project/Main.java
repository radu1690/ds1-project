package it.unitn.disi.ds1.project;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Main {
    final static int N_L1 = 3;
    final static int N_L2 = 3;
    final static int N_CLIENTS = 3;

    ActorRef database;
    List<ActorRef> cacheL1;
    List<ActorRef> cacheL2;
    ArrayList<ActorRef> clients;
    final static ActorSystem system = ActorSystem.create("caches");
    public Main(){
        initialize();
        inputContinue();

        testCriticalWrite();

//        System.out.println("WRITE VALUE 22 IN ID 0 TO A NODE THAT CRASHES");
//        Messages.CrashMsg crashMsg = new Messages.CrashMsg(Messages.CrashType.ReadRequest, Messages.CrashTime.MessageReceived);
//        cacheL2.get(0).tell(crashMsg, ActorRef.noSender());
////        inputContinue();
//        Messages.StartWriteRequestMsg w1 = new Messages.StartWriteRequestMsg(0, 22, cacheL2.get(0));
//        clients.get(0).tell(w1, ActorRef.noSender());
//
//        inputContinue();
//
//
//        checkEventualConsistency(0);
//
//        inputContinue();
//
//        System.out.println("READ ID 0 FROM CRASHED CACHE");
//        Messages.StartReadRequestMsg r1 = new Messages.StartReadRequestMsg(0, cacheL2.get(0));
//        clients.get(0).tell(r1, ActorRef.noSender());
//
////        Messages.StartReadRequestMsg r2 = new Messages.StartReadRequestMsg(0, cacheL2.get(2));
////        clients.get(0).tell(r2, ActorRef.noSender());
//
//        inputContinue();
//        System.out.println("TELL "+cacheL2.get(4).path().name()+" TO WRITE VALUE 33 IN ID 0");
//        Messages.StartWriteRequestMsg w2 = new Messages.StartWriteRequestMsg(0, 33, cacheL2.get(4));
//        clients.get(0).tell(w2, ActorRef.noSender());
//
//        inputContinue();
//
//        checkEventualConsistency(0);
//
//        inputContinue();
//        System.out.println("TELL "+cacheL2.get(4).path().name()+" TO WRITE VALUE 99 IN ID 0");
//        Messages.StartWriteRequestMsg w3 = new Messages.StartWriteRequestMsg(0, 99, cacheL2.get(4));
//        clients.get(0).tell(w3, ActorRef.noSender());
//
//        inputContinue();
//
//        checkEventualConsistency(0);
//
//        inputContinue();
//
//
//        System.out.println("TELL "+cacheL2.get(5).path().name()+" TO CRITICAL READ ID 0");
//        Messages.StartCritReadRequestMsg cr1 = new Messages.StartCritReadRequestMsg(0, cacheL2.get(5));
//        clients.get(2).tell(cr1, ActorRef.noSender());
//
//        inputContinue();
//        System.out.println("TELL "+cacheL2.get(5).path().name()+" TO CRITICAL WRITE VALUE 1690 IN ID 0");
//        Messages.StartCritWriteRequestMsg wc1 = new Messages.StartCritWriteRequestMsg(0, 1690, cacheL2.get(5));
//        clients.get(2).tell(wc1, ActorRef.noSender());
//
//
//
//
//        inputContinue();
//        checkEventualConsistency(0);
//
//        System.out.println("CRASH ON L1 READ");
//        Messages.CrashMsg crashL1 = new Messages.CrashMsg(Messages.CrashType.ReadRequest, Messages.CrashTime.MessageReceived);
//        cacheL1.get(0).tell(crashL1, ActorRef.noSender());
//        inputContinue();
//        Messages.StartReadRequestMsg readCrashL1 = new Messages.StartReadRequestMsg(0, cacheL2.get(0));
//        clients.get(0).tell(readCrashL1, ActorRef.noSender());
//
//        inputContinue();
//
//        System.out.println("CRASH ON L1 WRITE");
//        crashL1 = new Messages.CrashMsg(Messages.CrashType.WriteRequest, Messages.CrashTime.MessageReceived);
//        cacheL1.get(0).tell(crashL1, ActorRef.noSender());
//
//        inputContinue();
//
//        Messages.StartWriteRequestMsg writeCrashL1 = new Messages.StartWriteRequestMsg(0, 15, cacheL2.get(0));
//        clients.get(0).tell(writeCrashL1, ActorRef.noSender());
//
//
//        inputContinue();
//
//        checkEventualConsistency(0);


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
        System.out.println("TELL "+cacheL2.get(5).path().name()+" TO CRITICAL WRITE VALUE 1690 IN ID 0");
        Messages.StartCritWriteRequestMsg wc1 = new Messages.StartCritWriteRequestMsg(0, 1690, cacheL2.get(5));
        Messages.StartWriteRequestMsg w1 = new Messages.StartWriteRequestMsg(0, 1, cacheL2.get(6));
        Messages.StartWriteRequestMsg w2 = new Messages.StartWriteRequestMsg(0, 2, cacheL2.get(1));
        Messages.StartWriteRequestMsg w3 = new Messages.StartWriteRequestMsg(0, 3, cacheL2.get(2));
        Messages.StartWriteRequestMsg w4 = new Messages.StartWriteRequestMsg(0, 4, cacheL2.get(3));
        Messages.StartReadRequestMsg r1 = new Messages.StartReadRequestMsg(0, cacheL2.get(4));
        Messages.StartReadRequestMsg r2 = new Messages.StartReadRequestMsg(0, cacheL2.get(5));
        Messages.StartReadRequestMsg r3 = new Messages.StartReadRequestMsg(0, cacheL2.get(6));
        Messages.StartReadRequestMsg r4 = new Messages.StartReadRequestMsg(0, cacheL2.get(3));
        Messages.CrashMsg cr1 = new Messages.CrashMsg(Messages.CrashType.FlushRequest, Messages.CrashTime.MessageReceived);
        cacheL1.get(1).tell(cr1, ActorRef.noSender());

        clients.get(0).tell(w1, ActorRef.noSender());
        clients.get(0).tell(w2, ActorRef.noSender());
        clients.get(0).tell(w3, ActorRef.noSender());
        clients.get(0).tell(w4, ActorRef.noSender());

//        inputContinue();


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
        Messages.CheckConsistencyMsg msg = new Messages.CheckConsistencyMsg(dataId);
        database.tell(msg, ActorRef.noSender());
        for(ActorRef cache: cacheL1){
            cache.tell(msg, ActorRef.noSender());
        }
        for(ActorRef cache: cacheL2){
            cache.tell(msg, ActorRef.noSender());
        }
    }
}