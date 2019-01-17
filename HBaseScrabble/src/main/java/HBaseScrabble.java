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

    //Table name
    private static final String dbName = "Scrabble";
    private static final byte[] table = Bytes.toBytes(dbName);

    //Column Family name
    private static final byte[] columnFamily = Bytes.toBytes("Tournaments");

    //Column names
    private static final byte[] gameIdCol = Bytes.toBytes("gameid");
    private static final byte[] tourneyIdCol = Bytes.toBytes("tourneyid");
    private static final byte[] tieCol = Bytes.toBytes("tie");
    private static final byte[] winnerIdCol = Bytes.toBytes("winnerid");
    private static final byte[] winnerNameCol = Bytes.toBytes("winnername");
    private static final byte[] winnerScoreCol = Bytes.toBytes("winnerscore");
    private static final byte[] winnerOldCol = Bytes.toBytes("winneroldrating");
    private static final byte[] winnerNewCol = Bytes.toBytes("winnernewrating");
    private static final byte[] winnerPosCol = Bytes.toBytes("winnerpos");
    private static final byte[] looserIdCol = Bytes.toBytes("loserid");
    private static final byte[] loserNameCol = Bytes.toBytes("losername");
    private static final byte[] looserScoreCol = Bytes.toBytes("loserscore");
    private static final byte[] looserOldCol = Bytes.toBytes("loseroldrating");
    private static final byte[] loserNewCol = Bytes.toBytes("losernewrating");
    private static final byte[] looserPosCol = Bytes.toBytes("loserpos");
    private static final byte[] roundCol = Bytes.toBytes("round");
    private static final byte[] divisionCol = Bytes.toBytes("division");
    private static final byte[] dateCol = Bytes.toBytes("date");
    private static final byte[] lexiconCol = Bytes.toBytes("lexicon");

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

    /**
     * This method creates the table in HBase.
     * @throws IOException
     */
    public void createTable() throws IOException {
        HTableDescriptor hTable = new HTableDescriptor(TableName.valueOf(dbName));
        HColumnDescriptor family = new HColumnDescriptor(columnFamily);

        //Sets the number version to 10, default is 3
        family.setMaxVersions(10);
        hTable.addFamily(family);

        this.hBaseAdmin.createTable(hTable);
    }

    /**
     * This method loads the data from a CSV file and creates de structure of the column family.
     * @param folder //Complete route to the directory containing de CSV
     * @throws IOException
     */
    public void loadTable(String folder)throws IOException{

        List<Put> loadList = new ArrayList();
        String route = folder + "/scrabble_games.csv";
        Put put = null;
        int i = 0;
        int j = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(route))) {
            String line;

            while ((line = br.readLine()) != null) {
            String[] data = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

                if (j!= 0) {
                     //Row key
                     byte[] key = this.getKey(Bytes.toBytes(Integer.parseInt(data[1])),Bytes.toBytes(Integer.parseInt(data[0])));
                     put = new Put(key);

                     //Version uses system time as timestamp
                     long ts = System.currentTimeMillis();

                     //Adding data: Column Family, Column, Timestamp, Value
                     put.add(columnFamily, gameIdCol, ts, Bytes.toBytes(Integer.parseInt(data[0])));
                     put.add(columnFamily, tourneyIdCol, ts, Bytes.toBytes(Integer.parseInt(data[1])));
                     put.add(columnFamily, tieCol, ts, Bytes.toBytes(data[2]));
                     put.add(columnFamily, winnerIdCol, ts, Bytes.toBytes(Integer.parseInt(data[3])));
                     put.add(columnFamily, winnerNameCol, ts, Bytes.toBytes(data[4]));
                     put.add(columnFamily, winnerScoreCol, ts, Bytes.toBytes(Integer.parseInt(data[5])));
                     put.add(columnFamily, winnerOldCol, ts, Bytes.toBytes(Integer.parseInt(data[6])));
                     put.add(columnFamily, winnerNewCol, ts, Bytes.toBytes(Integer.parseInt(data[7])));
                     put.add(columnFamily, winnerPosCol, ts, Bytes.toBytes(Integer.parseInt(data[8])));
                     put.add(columnFamily, looserIdCol, ts, Bytes.toBytes(Integer.parseInt(data[9])));
                     put.add(columnFamily, loserNameCol, ts, Bytes.toBytes(data[10]));
                     put.add(columnFamily, looserScoreCol, ts, Bytes.toBytes(Integer.parseInt(data[11])));
                     put.add(columnFamily, looserOldCol, ts, Bytes.toBytes(Integer.parseInt(data[12])));
                     put.add(columnFamily, loserNewCol, ts, Bytes.toBytes(Integer.parseInt(data[13])));
                     put.add(columnFamily, looserPosCol, ts, Bytes.toBytes(Integer.parseInt(data[14])));
                     put.add(columnFamily, roundCol, ts, Bytes.toBytes(Integer.parseInt(data[15])));
                     put.add(columnFamily, divisionCol, ts, Bytes.toBytes(Integer.parseInt(data[16])));
                     put.add(columnFamily, dateCol, ts, Bytes.toBytes(data[17]));
                     put.add(columnFamily, lexiconCol, ts, Bytes.toBytes(data[18]));

                    loadList.add(put);

                    //Inserts data in batches of 5,000
                    if(i % 5000 == 0) {
                        hTable.put(loadList);
                        loadList = new ArrayList();
                    }
                    i++;
                }
                j++;
            }
            hTable.put(put);
        }
        System.exit(-1);
    }

    /**
     * This method generates the key
     * @param tournamentIdKey
     * @param gameIdKey
     * @return The encoded key to be inserted in HBase
     */
    private byte[] getKey(byte[] tournamentIdKey, byte[] gameIdKey){

        //Sets the key to a fixed size of the total of 2 ints (expeceted size of each arg is 4)
        byte[] key = new byte[8];

        //First part: tournamentid
        key[0] = tournamentIdKey[0];
        key[1] = tournamentIdKey[1];
        key[2] = tournamentIdKey[2];
        key[3] = tournamentIdKey[3];

        //Second part: gameid
        key[4] = gameIdKey[0];
        key[5] = gameIdKey[1];
        key[6] = gameIdKey[2];
        key[7] = gameIdKey[3];

        return key;
    }


    /**
     * This method generates Query1
     * @param tourneyid Tournament number
     * @param winnername Name of the winner
     * @return Returns all the opponents (Loserid) of a given Winnername in a tournament (Tourneyid).
     */
    public List<String> query1(String tourneyid, String winnername) throws IOException {

        List<String>  finalResult = new ArrayList<>();
        int nextTournament = Integer.parseInt(tourneyid) + 1;

        //Defining the start as the tournament and the end until the next one starts (exclusive)
        byte[] startKey = this.getKey(Bytes.toBytes(Integer.parseInt(tourneyid)), Bytes.toBytes(0));
        byte[] endKey = this.getKey(Bytes.toBytes(nextTournament), Bytes.toBytes(0));

        //Select columns
        Scan scan = new Scan(startKey, endKey);
        scan.addColumn(columnFamily,winnerNameCol);
        scan.addColumn(columnFamily,looserIdCol);

        //Where condition
        Filter filter = new SingleColumnValueFilter(columnFamily, winnerNameCol, CompareFilter.CompareOp.EQUAL,Bytes.toBytes(winnername));
        scan.setFilter(filter);

        ResultScanner result = hTable.getScanner(scan);
        Result res = result.next();

        //Iterating over the results and adding them to a final list
        while (res!= null && !res.isEmpty()) {

            int id = Bytes.toInt(res.getValue(columnFamily, looserIdCol));
            finalResult.add(String.valueOf(id));

            res = result.next();
        }
        result.close();
        return  finalResult;
    }

    /**
     * This method generates Query2
     * @param firsttourneyid Initial tournament
     * @param lasttourneyid Final tournament
     * @return Returns the ids of the players (winner and loser) that have participated more than once
     * in all tournaments between two given Tourneyids.
     */
    public List<String> query2(String firsttourneyid, String lasttourneyid) throws IOException {

        List<String>  finalResult = new ArrayList<>();
	    Map<Integer, Map<Integer, Integer>> hmap = new HashMap<>();
        int totalTournaments = (Integer.parseInt(lasttourneyid)) - (Integer.parseInt(firsttourneyid));

        //Defining the start and end tournament (exclusive)
        byte[] startKey = this.getKey(Bytes.toBytes(Integer.parseInt(firsttourneyid)), Bytes.toBytes(0));
        byte[] endKey = this.getKey(Bytes.toBytes(Integer.parseInt(lasttourneyid))  , Bytes.toBytes(0));

        //Select columns
        Scan scan = new Scan(startKey, endKey);
        scan.addColumn(columnFamily,tourneyIdCol);
        scan.addColumn(columnFamily,winnerIdCol);
        scan.addColumn(columnFamily,looserIdCol);

        ResultScanner result = hTable.getScanner(scan);
        Result res = result.next();

        //Iterating over the results and adding them to a Map of Maps
        while (res!= null && !res.isEmpty()) {

            int tourneyId = Bytes.toInt(res.getValue(columnFamily, tourneyIdCol));
            int looserId = Bytes.toInt(res.getValue(columnFamily, looserIdCol));
            int winnerId = Bytes.toInt(res.getValue(columnFamily, winnerIdCol));

            //Merge winners and loosers into a single player array
            for (int playerId: new int[] {looserId, winnerId}) {

                hmap.putIfAbsent(playerId, new HashMap<>());
                Map<Integer,Integer> tourneys = hmap.get(playerId);
                Integer tournamentVal = tourneys.get(tourneyId);

                //Counting the number of tournaments of the player
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

        //Iterating over parent Map
        for (Map.Entry<Integer, Map<Integer, Integer>> entry : hmap.entrySet()) {
            Map<Integer, Integer> tourneysPerPlayerMap = entry.getValue();

            //Player Id as a key
            int playerId = entry.getKey();
            int timesParticipated = 0;
			Collection<Integer> tourneysPerPlayer = tourneysPerPlayerMap.values();

            //Counting the number of times the player participated
			for (Integer countPerGame : tourneysPerPlayer) {
				if (countPerGame >= 2) {
					timesParticipated++;
				}
			}

			//Adding to final list if the playes has more than 2 occurrences in all tourneys
			if (timesParticipated == totalTournaments) {
				finalResult.add(String.valueOf(String.valueOf(playerId)));
			}
        }
        return  finalResult;
    }

    /**
     * This method generates Query3
     * @param tourneyid Tournament number
     * @return Given a Tourneyid, the query returns the Gameid, the ids of the two participants that
     * have finished in tie.
     */
    public List<String> query3(String tourneyid) throws IOException {

        List<String>  finalResult = new ArrayList<>();
        int nextTournament = Integer.parseInt(tourneyid) + 1;

        //Defining the start as the tournament and the end until the next one starts (exclusive)
        byte[] startKey = this.getKey(Bytes.toBytes(Integer.parseInt(tourneyid)), Bytes.toBytes(0));
        byte[] endKey = this.getKey(Bytes.toBytes(nextTournament), Bytes.toBytes(0));

        //Select columns
        Scan scan = new Scan(startKey, endKey);
        scan.addColumn(columnFamily,looserIdCol);
        scan.addColumn(columnFamily,gameIdCol);
        scan.addColumn(columnFamily,winnerIdCol);
        scan.addColumn(columnFamily,tieCol);

        //Where condition
        Filter filter = new SingleColumnValueFilter(columnFamily, tieCol, CompareFilter.CompareOp.EQUAL,Bytes.toBytes("True"));
        scan.setFilter(filter);

        ResultScanner result = hTable.getScanner(scan);
        Result res = result.next();

        //Iterating over the results and adding them to a final list
        while (res!= null && !res.isEmpty()) {

            int gameId = Bytes.toInt(res.getValue(columnFamily, gameIdCol));
            int winnerId = Bytes.toInt(res.getValue(columnFamily, winnerIdCol));
            int looserId = Bytes.toInt(res.getValue(columnFamily, looserIdCol));
            finalResult.add(String.valueOf("Game:" + String.valueOf(gameId) + " - Players:" + String.valueOf(winnerId) + " and " + String.valueOf(looserId)));

            res = result.next();
        }
        result.close();
        return  finalResult;
     }


    //Main
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
