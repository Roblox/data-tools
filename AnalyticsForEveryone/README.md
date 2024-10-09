# Analytics for Everyone

Here you can find reference material for a Druid summit talk https://druidsummit.org/agenda/

This directory holds two plugins we use at Roblox for the usage of Approximate Datasketches. Our data platform primarily uses Spark and Trino so we built custom udaf extensions for apache datasketches to ensure a binary compatible datasketch that we can share from our C++ engine code all the way to Spark jobs or Trino queries. You can find out plenty more about these sketches at https://datasketches.apache.org/

Additionally in the druid_workflow piece you can find example data and usage of datasketch data in druid.