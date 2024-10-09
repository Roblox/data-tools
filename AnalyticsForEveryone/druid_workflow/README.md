## Using approximate datasketches in druid

1. To start use the druid distribution from the master branch of the druid repo. Instructions for setting this up are located at [Druid Docker Distribution](https://github.com/apache/druid/tree/master/distribution/docker)

2. To try out loading data into your running druid docker image you can look at the [sql/ingest](./sql/ingest) directory for how to [rollup datasketches](./sql/ingest/insert_rollup.sql) via druid or how to directly load pre-aggregated [datasketch data](./sql/ingest/sketch_insert_local.sql) into your druid installation. The last ingest is a near exact copy of how this insert is actually created by our airflow plugin. For these inserts we use a close fork to the official [airflow druid provider](https://airflow.apache.org/docs/apache-airflow-providers-apache-druid/stable/index.html).

3. Once you've loaded data into druid you can try out some of the [sql/query](./sql/query) queries.
    - [mau example](./sql/query/mau_query_example.sql) is a near exact copy of the actually generated query from our [Cube.dev](https://cube.dev/) fork. 
    - [dau_by_country.sql](./sql/query/dau_by_country.sql) is a simple query you can use against the example data to calculate dau for a particular universe. This can easily be changed to do breakdowns, filters, or any typical sql logic.
    - [simplified_mau.sql](./sql/query/simplified_mau.sql) shows off a simplified version of mau that still demonstrates the inline query elements of our MAU generation that move work to historical nodes.

4. You can also look into some of our cube.dev setup in the [cube](./cube/schemas/) directory. This has the schema for converting druid schemas towards dataframe (measure + dimension) style queries.



Follow this video for a simple demo of some queries