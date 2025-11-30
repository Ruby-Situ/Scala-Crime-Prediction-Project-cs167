--- Task 1: Spatial Analysis ---
bin/beast --conf spark.executor.memory=16g --conf spark.driver.memory=16g \
--class edu.ucr.cs.cs167.team.DataPreparation src/group11-1.0-SNAPSHOT.jar Chicago_Crimes_1k.csv.bz2

--- Task 3: Temporal Analysis ---
bin/beast --conf spark.executor.memory=16g --conf spark.driver.memory=16g \
--class edu.ucr.cs.cs167.team.BeastScala src/group11-1.0-SNAPSHOT.jar Chicago_Crimes_ZIP 01/01/2000 12/31/2023

--- Task 4: Spatial Analysis ---
bin/beast --conf spark.executor.memory=16g --conf spark.driver.memory=16g \
--class edu.ucr.cs.cs167.team.CrimeRangeReport src/group11-1.0-SNAPSHOT.jar Chicago_Crimes_ZIP

--- Task 5: Arrest Prediction ---
spark-submit --conf spark.executor.memory=16g --conf spark.driver.memory=16g \
--class edu.ucr.cs.cs167.team.CrimeArrestPrediction --master "local[*]" \
src/group11-1.0-SNAPSHOT.jar Chicago_Crimes_ZIP
idk i cant run them on my vs code for some reason
