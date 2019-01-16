import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import javax.crypto.KeyGenerator;
import java.io.*;
import java.util.*;
import java.util.HashMap;


public class HBaseScrabble {
    private Configuration config;
    private HBaseAdmin hBaseAdmin;
    private static final String DB_NAME = "SCRABBLE";
    private byte[] table = Bytes.toBytes(DB_NAME);
    byte[] cf = Bytes.toBytes("TOURNAMENTS");

    HTable hTable;
    /**
     * The Constructor. Establishes the connection with HBase.
     * @param zkHost
     * @throws IOException
     */
    public HBaseScrabble(String zkHost) throws IOException {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zkHost.split(":")[0]);
        config.set("hbase.zookeeper.property.clientPort", zkHost.split(":")[1]);
        HBaseConfiguration.addHbaseResources(config);
        this.hBaseAdmin = new HBaseAdmin(config);
        hTable = new HTable(config,table);
    }

    public void createTable() throws IOException {


        HTableDescriptor hTable = new HTableDescriptor(TableName.valueOf(DB_NAME));
        HColumnDescriptor family = new HColumnDescriptor(cf);
        family.setMaxVersions(10); // Default is 3.
        hTable.addFamily(family);

        this.hBaseAdmin.createTable(hTable);
    }

    public void loadTable(String folder)throws IOException{


        List<Put> listPuts = new ArrayList();
        Put put=null;
        int i = 0;
       // List<List<String>> records = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(folder + "/scrabble_games.csv"))) {
            String line;
	     int x = 0;
            while ((line = br.readLine()) != null) {
            String[] values = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
          //  records.add(Arrays.asList(values));
                if (x!= 0) {
                    byte[] key = this.getKey(Bytes.toBytes(Integer.parseInt(values[1])),Bytes.toBytes(Integer.parseInt(values[0])));
                   // byte[] key = Bytes.toBytes(values[1]+values[0]);
                    byte[] gameId = Bytes.toBytes("GAME_ID");
                    byte[] tourneyId = Bytes.toBytes("TOURNEY_ID");
                    byte[] column1 = Bytes.toBytes("TIE");
                    byte[] column2 = Bytes.toBytes("WINNER_ID");
                    byte[] column3 = Bytes.toBytes("WINNER_NAME");
                    byte[] column4 = Bytes.toBytes("WINNER_SCORE");
                    byte[] column5 = Bytes.toBytes("WINNER_OLD_RATING");
                    byte[] column6 = Bytes.toBytes("WINNER_NEW_RATING");
                    byte[] column7 = Bytes.toBytes("WINNER_POS");
                    byte[] column8 = Bytes.toBytes("LOOSER_ID");
                    byte[] column9 = Bytes.toBytes("LOOSER_NAME");
                    byte[] column10 = Bytes.toBytes("LOOSER_SCORE");
                    byte[] column11 = Bytes.toBytes("LOOSER_OLD_RATING");
                    byte[] column12 = Bytes.toBytes("LOOSER_NEW_RATING");
                    byte[] column13 = Bytes.toBytes("LOOSER_POS");
                    byte[] column14 = Bytes.toBytes("ROUND");
                    byte[] column15 = Bytes.toBytes("DIVISION");
                    byte[] column16 = Bytes.toBytes("DATE");
                    byte[] column17 = Bytes.toBytes("LEXICON");
                     put = new Put(key);
                     long ts = System.currentTimeMillis();
                    put.add(cf, gameId, ts, Bytes.toBytes(Integer.parseInt(values[0])));
                    put.add(cf, tourneyId, ts, Bytes.toBytes(Integer.parseInt(values[1])));
                    put.add(cf, column1, ts, Bytes.toBytes(values[2]));
                     put.add(cf, column2, ts, Bytes.toBytes(Integer.parseInt(values[3])));
                     put.add(cf, column3, ts, Bytes.toBytes(values[4]));
                     put.add(cf, column4, ts, Bytes.toBytes(Integer.parseInt(values[5])));
                     put.add(cf, column5, ts, Bytes.toBytes(Integer.parseInt(values[6])));
                     put.add(cf, column6, ts, Bytes.toBytes(Integer.parseInt(values[7])));
                     put.add(cf, column7, ts, Bytes.toBytes(Integer.parseInt(values[8])));
                     put.add(cf, column8, ts, Bytes.toBytes(Integer.parseInt(values[9])));
                     put.add(cf, column9, ts, Bytes.toBytes(values[10]));
                     put.add(cf, column10, ts, Bytes.toBytes(Integer.parseInt(values[11])));
                     put.add(cf, column11, ts, Bytes.toBytes(Integer.parseInt(values[12])));
                     put.add(cf, column12, ts, Bytes.toBytes(Integer.parseInt(values[13])));
                     put.add(cf, column13, ts, Bytes.toBytes(Integer.parseInt(values[14])));
                     put.add(cf, column14, ts, Bytes.toBytes(Integer.parseInt(values[15])));
                     put.add(cf, column15, ts, Bytes.toBytes(Integer.parseInt(values[16])));
                     put.add(cf, column16, ts, Bytes.toBytes(values[17]));
                     put.add(cf, column17, ts, Bytes.toBytes(values[18]));
                    listPuts.add(put);
                    if(i % 10000 == 0) {
                        hTable.put(listPuts);
                        listPuts = new ArrayList();
                    }
                    i++;
                }
                x++;

            }
            hTable.put(put);
        }

        System.out.println("Finished successful");
        System.exit(-1);
    }



    /**
     * This method generates the key
     * @param values The value of each column
     * @param keyTable The position of each value that is required to create the key in the array of values.
     * @return The encoded key to be inserted in HBase
     */
    private byte[] getKey(String[] values, int[] keyTable) {
        String keyString = "";
        for (int keyId : keyTable){
            keyString += Bytes.toBytes(values[keyId]);
        }
        byte[] key = Bytes.toBytes(keyString);

        return key;
    }


    private byte[] getKey(byte[] key1, byte[] key2){

        byte[] key = new byte[8];

        key[0] = key1[0];
        key[1] = key1[1];
        key[2] = key1[2];
        key[3] = key1[3];
        key[4] = key2[0];
        key[5] = key2[1];
        key[6] = key2[2];
        key[7] = key2[3];

        return key;
    }


        public List<String> query1(String tourneyid, String winnername) throws IOException {

            List<String>  list = new ArrayList<>();

            int tourneyPlusOne = Integer.parseInt(tourneyid) +1;

            byte[] key = this.getKey(Bytes.toBytes(Integer.parseInt(tourneyid)), Bytes.toBytes(0));
            byte[] key2 = this.getKey(Bytes.toBytes(tourneyPlusOne)  , Bytes.toBytes(0));

            Scan scan = new Scan(key, key2);
            scan.addColumn(cf,Bytes.toBytes("WINNER_NAME"));
            scan.addColumn(cf,Bytes.toBytes("LOOSER_ID"));

            Filter filter = new SingleColumnValueFilter(cf, Bytes.toBytes("WINNER_NAME"), CompareFilter.CompareOp.EQUAL,Bytes.toBytes(winnername));
            scan.setFilter(filter);
            ResultScanner result = hTable.getScanner(scan);

            Result res = result.next();


            while (res!= null && !res.isEmpty()) {

                int id = Bytes.toInt(res.getValue(cf, Bytes.toBytes("LOOSER_ID")));
                list.add(String.valueOf(id));

                res = result.next();
            }
            result.close();
            return  list;
        }

    public List<String> query2(String firsttourneyid, String lasttourneyid) throws IOException {

            List<String>  list = new ArrayList<>();
    	    Map<Integer, Map<Integer, Integer>> hmap = new HashMap<>();
            int totalTournaments = (Integer.parseInt(lasttourneyid)) - (Integer.parseInt(firsttourneyid));

            byte[] key = this.getKey(Bytes.toBytes(Integer.parseInt(firsttourneyid)), Bytes.toBytes(0));
            byte[] key2 = this.getKey(Bytes.toBytes(Integer.parseInt(lasttourneyid))  , Bytes.toBytes(0));

            Scan scan = new Scan(key, key2);
            scan.addColumn(cf,Bytes.toBytes("WINNER_ID"));
            scan.addColumn(cf,Bytes.toBytes("LOOSER_ID"));
            scan.addColumn(cf,Bytes.toBytes("TOURNEY_ID"));

            ResultScanner result = hTable.getScanner(scan);
            Result res = result.next();

            while (res!= null && !res.isEmpty()) {

                int looserId = Bytes.toInt(res.getValue(cf, Bytes.toBytes("LOOSER_ID")));
                int winnerId = Bytes.toInt(res.getValue(cf, Bytes.toBytes("WINNER_ID")));
    	        int tourneyId = Bytes.toInt(res.getValue(cf, Bytes.toBytes("TOURNEY_ID")));

                for (int playerId: new int[] {looserId, winnerId}) {

                    hmap.putIfAbsent(playerId, new HashMap<>());
                    Map<Integer,Integer> tourneys = hmap.get(playerId);
                    Integer tournamentVal = tourneys.get(tourneyId);

                    if (tournamentVal == null) {
                        tourneys.put(tourneyId, 1);
                    }
                    else {
                        tourneys.put(tourneyId, tournamentVal + 1);
                    }
                }
                res = result.next();
            }
            result.close();

            //Iterating over parentMap
            for (Map.Entry<Integer, Map<Integer, Integer>> entry : hmap.entrySet()) {

                Map<Integer, Integer> tourneysPerPlayerMap = entry.getValue();

                //Player Id as a key
                int playerId = entry.getKey();

				Collection<Integer> tourneysPerPlayer = tourneysPerPlayerMap.values();

				int timesParticipatedMoreThanOne = 0;
				for (Integer countPerGame : tourneysPerPlayer) {
					if (countPerGame >= 2) {
						timesParticipatedMoreThanOne++;
					}
				}

				//Adding to final list if the playes has more than 2 occurrences in all tourneys
				if (timesParticipatedMoreThanOne == totalTournaments) {
					list.add(String.valueOf(String.valueOf(playerId)));
				}
            }

            return  list;
        }

    public List<String> query3(String tourneyid) throws IOException {
        List<String>  list = new ArrayList<>();

        int tourneyPlusOne = Integer.parseInt(tourneyid) +1;

        byte[] key = this.getKey(Bytes.toBytes(Integer.parseInt(tourneyid)), Bytes.toBytes(0));
        byte[] key2 = this.getKey(Bytes.toBytes(tourneyPlusOne)  , Bytes.toBytes(0));

        Scan scan = new Scan(key, key2);
        scan.addColumn(cf,Bytes.toBytes("LOOSER_ID"));
        scan.addColumn(cf,Bytes.toBytes("GAME_ID"));
        scan.addColumn(cf,Bytes.toBytes("WINNER_ID"));
        scan.addColumn(cf,Bytes.toBytes("TIE"));

        Filter filter = new SingleColumnValueFilter(cf, Bytes.toBytes("TIE"), CompareFilter.CompareOp.EQUAL,Bytes.toBytes("True"));
        scan.setFilter(filter);
        ResultScanner result = hTable.getScanner(scan);

        Result res = result.next();


        while (res!= null && !res.isEmpty()) {

            int gameId = Bytes.toInt(res.getValue(cf, Bytes.toBytes("GAME_ID")));
            int looserId = Bytes.toInt(res.getValue(cf, Bytes.toBytes("LOOSER_ID")));
            int winnerId = Bytes.toInt(res.getValue(cf, Bytes.toBytes("WINNER_ID")));

            list.add(String.valueOf(String.valueOf(gameId) + "-" + String.valueOf(looserId) + "-" + String.valueOf(winnerId)));

            res = result.next();
        }
        result.close();
        return  list;
     }


    public static void main(String[] args) throws IOException {
        if(args.length<2){
            System.out.println("Error: \n1)ZK_HOST:ZK_PORT, \n2)action [createTable, loadTable, query1, query2, query3], \n3)Extra parameters for loadTables and queries:\n" +
                    "\ta) If loadTable: csvsFolder.\n " +
                    "\tb) If query1: tourneyid winnername.\n  " +
                    "\tc) If query2: firsttourneyid lasttourneyid.\n  " +
                    "\td) If query3: tourneyid.\n  ");
            System.exit(-1);
        }
        HBaseScrabble hBaseScrabble = new HBaseScrabble(args[0]);
        if(args[1].toUpperCase().equals("CREATETABLE")){
            hBaseScrabble.createTable();
        }
        else if(args[1].toUpperCase().equals("LOADTABLE")){
            if(args.length!=3){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)action [createTables, loadTables], 3)csvsFolder");
                System.exit(-1);
            }
            else if(!(new File(args[2])).isDirectory()){
                System.out.println("Error: Folder "+args[2]+" does not exist.");
                System.exit(-2);
            }
            hBaseScrabble.loadTable(args[2]);
        }
        else if(args[1].toUpperCase().equals("QUERY1")){
            if(args.length!=4){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)query1, " +
                        "3) tourneyid 4) winnername");
                System.exit(-1);
            }

            List<String> opponentsName = hBaseScrabble.query1(args[2], args[3]);
            System.out.println("There are "+opponentsName.size()+" opponents of winner "+args[3]+" that play in tourney "+args[2]+".");
            System.out.println("The list of opponents is: "+Arrays.toString(opponentsName.toArray(new String[opponentsName.size()])));
        }

        else if(args[1].toUpperCase().equals("QUERY2")){
            if(args.length!=4){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)query2, " +
                        "3) firsttourneyid 4) lasttourneyid");
                System.exit(-1);
            }
            List<String> playerNames =hBaseScrabble.query2(args[2], args[3]);
            System.out.println("There are "+playerNames.size()+" players that participates in more than one tourney between tourneyid "+args[2]+" and tourneyid "+args[3]+" .");
            System.out.println("The list of players is: "+Arrays.toString(playerNames.toArray(new String[playerNames.size()])));
        }
        else if(args[1].toUpperCase().equals("QUERY3")){
            if(args.length!=3){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2) query3, " +
                        "3) tourneyid");
                System.exit(-1);
            }
            List<String> games = hBaseScrabble.query3(args[2]);
            System.out.println("There are "+games.size()+" that ends in tie in tourneyid "+args[2]+" .");
            System.out.println("The list of games is: "+Arrays.toString(games.toArray(new String[games.size()])));
        }
        else{
            System.out.println("Error: \n1)ZK_HOST:ZK_PORT, \n2)action [createTable, loadTable, query1, query2, query3], \n3)Extra parameters for loadTables and queries:\n" +
                    "\ta) If loadTable: csvsFolder.\n " +
                    "\tb) If query1: tourneyid winnername.\n  " +
                    "\tc) If query2: firsttourneyid lasttourneyid.\n  " +
                    "\td) If query3: tourneyid.\n  ");
            System.exit(-1);
        }

    }



}
