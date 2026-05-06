# Silver leest Bronze via Change Data Feed + apply_changes

Bronze schrijft in twee modes (`full` overschrijft, `incremental` appendt). Een gewone streaming-read op een tabel die wordt overschreven faalt. Bronze-tabellen krijgen daarom `delta.enableChangeDataFeed=true`, en Silver consumeert die CDF via `spark.readStream.option("readChangeFeed", "true")` + DLT's `apply_changes` — overschrijvingen verschijnen als `delete_row` + `insert_row` events en worden declaratief gemerged naar de Silver-tabellen.

Een eenvoudiger alternatief — alle Silver-tabellen als Materialised Views met batch-reads — is overwogen en afgewezen: het verliest het canonieke "streaming for cleansing"-patroon, biedt minder demo-waarde (CDF + `apply_changes` zijn op zichzelf showcase-features), en het CDC-patroon dat hier ontstaat is exact wat de geparkeerde SQL Server-bron via Lakeflow Connect later zal nodig hebben — geen herontwerp dus wanneer die unparked wordt.
