USE esidorov_hw4;

DROP TABLE if EXISTS aggregate;

create external table aggregate(
 date STRING,
 game_size INT,
 match_id STRING,
 match_mode STRING,
 party_size INT,
 player_assists INT,
 player_dbno INT,
 player_dist_ride Double,
 player_dist_walk Double,
 player_dmg Int,
 player_kills Int,
 player_name String,
 player_survive_time Double,
 team_id INT,
 team_placement INT
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY  ','
STORED AS TEXTFILE
LOCATION '/user/pakhtyamov/PubgDataDir/aggregate';

DROP TABLE if EXISTS deaths;

CREATE EXTERNAL TABLE deaths(
 killed_by STRING,
 killer_name STRING,
 killer_position_x DOUBLE,
 killer_position_y DOUBLE,
 map_name STRING,
 match_id STRING,
 time INT,
 victim_name STRING,
 victim_position_x DOUBLE,
 victim_position_y DOUBLE
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY  ','
STORED AS TEXTFILE
LOCATION '/user/pakhtyamov/PubgDataDir/deaths';

SELECT aggregate. team_placement, AVG(deaths.victim_position_x),
 AVG(deaths.victim_position_y), STDDEV(deaths.victim_position_x
), STDDEV(deaths.victim_position_y)
FROM aggregate INNER JOIN deaths ON aggregate.player_name = deaths.victim_name AND aggregate.match_id = deaths.match_id
WHERE aggregate. team_placement >= 2 AND aggregate. team_placement <= 4
GROUP BY aggregate.team_placement;