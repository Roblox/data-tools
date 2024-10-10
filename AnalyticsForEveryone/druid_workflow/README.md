## Using approximate datasketches in druid

1. To start you can boot up the druid docker image in this repo with `docker compose up` this is set up via this [getting started guide](https://druid.apache.org/docs/latest/tutorials/docker/) so follow that to tweak the docker setup. You must have the "druid-datasketches" and "druid-parquet-extensions" in the environment file to load the data.

2. To try out loading data into your running druid docker deployment you can look at the [sql/ingest](./sql/ingest) directory for how to ingest [datasketches via rollup](./sql/ingest/insert_rollup.sql) or how to directly load pre-aggregated [datasketch data](./sql/ingest/sketch_insert_local.sql). The last ingest is a near exact copy of how this insert is actually created by our airflow plugin. For these inserts we use a close fork to the official [airflow druid provider](https://airflow.apache.org/docs/apache-airflow-providers-apache-druid/stable/index.html) which monitors the query awaiting success before completing the task.

3. The sample parquet data is generated via test code in the spark-datasketches [library](../datasketches/datasketches-parent/spark-datasketches/src/test/scala/com/roblox/spark/sketches/DataGenerator.scala) in this repo. It is a reasonably small amount of data, but more could easily be generated to do bigger tests.

4. Once you've loaded data into druid you can try out some of the [sql/query](./sql/query) queries.
    - [mau example](./sql/query/mau_query_example.sql) is a near exact copy of the actually generated query from our [Cube.dev](https://cube.dev/) fork. This query would need to be adapted to work out of the box.
    - [dau_by_country.sql](./sql/query/dau_by_country.sql) is a simple query you can use against the example data to calculate dau for a particular universe. This can easily be changed to do breakdowns, filters, or any typical sql logic.
    - [simplified_mau.sql](./sql/query/simplified_mau.sql) shows off a simplified version of mau that demonstrates the inline query elements of our MAU generation that move work to historical nodes.

5. You can also look into some of our [cube.dev](https://cube.dev/) setup in the [cube](./cube/schemas/) directory. This has the schema for converting druid schemas to dataframe (measure + dimension) style queries.