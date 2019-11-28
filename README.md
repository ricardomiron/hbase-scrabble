# Apache HBase: Scrabble Games project
HBase is an open-source non-relational distributed database modeled after Google's [Bigtable](https://cloud.google.com/bigtable). It is developed as part of Apache Software Foundation's Apache Hadoop project and runs on top of HDFS :)

## Description
The goal of this project is to design the schema of an HBase table and develop a Java program that implements a set of queries on that HBase table. The table, called ScrabbleGames, stores information of a [scrabble tournament](http://www.cross-tables.com/). The ScrabbleGames table stores this information according to this [repo](https://github.com/fivethirtyeight/data/tree/master/scrabble-games)

Header | Definition
---|---------
`gameid` | A numerical game ID
`tourneyid` | A numerical tournament ID
`tie` | A binary variable indicating if the game ended in a tie
`winnerid` | A numerical ID for the winning player
`winnername` | The name of the winning player
`winnerscore` | The score of the winning player
`winneroldrating` | The winner’s rating before the game
`winnernewrating` | The winner’s rating after the game
`winnerpos` | The winner’s position in the tournament
`loserid` | A numerical ID for the losing player
`loserscore` | The score of the losing player
`loseroldrating` | The loser’s rating before the game
`losernewrating` | The loser’s rating after the game
`loserpos` | The loser’s position in the tournament
`round` | The round of the tournament in which the game took place
`division` | The division of the tournament in which the game took place
`date` | The date of the game
`lexicon` | A binary variable indicating if the game’s lexicon was the main North American lexicon (`False`) or the international lexicon (`True`)

Source: [cross-tables.com](http://cross-tables.com)

## Querys
The project is a Java program that uses HBase to create and load the table, and implements
the following queries:

- Query1: Returns all the opponents (Loserid) of a given Winnername in a tournament (Tourneyid).
- Query2: Returns the ids of the players (winner and loser) that have participated more than once in all tournaments between two given Tourneyids.
- Query3: Given a Tourneyid, the query returns the Gameid, the ids of the two participants that have finished in tie.
