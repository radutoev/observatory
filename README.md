###Stations 

stations.csv

`STN Identifier | WBAN Indentifier | Lat | Long`

Uiquely identified by the compoung key (STD, WBAN)

Possible values:

* 010013,,,
* 724017,03707,+37.358,-078.438
* 724017,,+37.350,-078.433

### Temperatures

`STN | WBAN | Month | Day | Temp (in Fahrenheit)`

* 010013,,11,25,39.2 -> The average temperature was 39.2 degrees Fahrenheit on November 25th at the station whose STN identifier is 010013.
* 724017,,08,11,81.14 -> The average temperature was 81.1 °F on August 11th at the station whose STN identifier is 724017.
* 724017,03707,12,06,32 -> The average temperature was 32 °F on December 6th at the station whose WBAN identifier is 03707.
* 724017,03707,01,29,35.6 -> At the same station, the average temperature was 35.6 °F on January 29th.

If temp is missing the value is set to 9999.9 