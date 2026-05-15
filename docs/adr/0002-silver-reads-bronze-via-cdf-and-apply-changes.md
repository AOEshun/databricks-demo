# Silver leest Bronze via Change Data Feed + apply_changes

> **Status update (2026-05-15):** Amended by [ADR-0010](0010-dw-captures-history-via-apply-changes-into.md).
>
> The underlying principle — the integration layer (formerly "Silver") reads from staging (formerly "Bronze") via Change Data Feed — still holds. The mechanism is now specific: `STREAM table_changes(<table>, <version>)` reads, feeding `FLOW AUTO CDC ... STORED AS SCD TYPE 2` via `SEQUENCE BY _commit_timestamp`. Earlier `apply_changes` references in this ADR refer to the same mechanism (renamed to `FLOW AUTO CDC`). Read ADR-0010 for the binding decision on history mechanism.

Bronze schrijft in twee modes (`full` overschrijft, `incremental` appendt). Een gewone streaming-read op een tabel die wordt overschreven faalt. Bronze-tabellen krijgen daarom `delta.enableChangeDataFeed=true`, en Silver consumeert die CDF via `spark.readStream.option("readChangeFeed", "true")` + DLT's `FLOW AUTO CDC` — overschrijvingen verschijnen als `delete_row` + `insert_row` events en worden declaratief gemerged naar de Silver-tabellen.

Een eenvoudiger alternatief — alle Silver-tabellen als Materialised Views met batch-reads — is overwogen en afgewezen: het verliest het canonieke "streaming for cleansing"-patroon, biedt minder demo-waarde (CDF + `FLOW AUTO CDC` zijn op zichzelf showcase-features), en het CDC-patroon dat hier ontstaat is exact wat de geparkeerde SQL Server-bron via Lakeflow Connect later zal nodig hebben — geen herontwerp dus wanneer die unparked wordt.
