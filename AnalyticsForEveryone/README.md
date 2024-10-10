# Analytics for Everyone

Here you can find reference material for a Druid summit 2024 talk given by Willis Kennedy https://druidsummit.org/agenda/

## Datasketches

This directory holds two plugins we use at Roblox that wrap Approximate Datasketches. Our data platform primarily uses Spark and Trino so we built custom udaf (user defined aggregate function) extensions for apache datasketches to ensure a binary compatible datasketch that we can share from our C++ engine code all the way to Spark jobs or Trino queries. You can find out plenty more about these sketches at https://datasketches.apache.org/


## Druid Workflow

In the workflow directory you can find an example docker compose that will stand up druid along with details about how to ingest some sample data using sql statements in this repo. Feel free to adapt or re-use these sql statements for your own queries or ingestion.