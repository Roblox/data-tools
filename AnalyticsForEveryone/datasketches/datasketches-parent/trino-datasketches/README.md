# Installation

Follow Trino standard [plugin deployment](https://trino.io/docs/current/develop/spi-overview.html). This plugin needs to be available on all nodes so however you deploy your trino cluster it will need to be included. We deploy this with the trino version specified in the pom.xml in this repo.

# DataSketches UDFs

[Apache DataSketches](https://datasketches.apache.org/) is a library of highly efficient data streaming algorithms for running approximate queries on very large datasets. 
These algorithms create data structures called "sketches", which aggregate data to be queried efficiently.
These UDFs provide interfaces to HLL, Theta, and KLL sketches. For more information on DataSketches, see the [official documentation](https://datasketches.apache.org/docs/Background/TheChallenge.html).

We provide:
* HLL Sketches - Count Distinct Elements (Fast)
* Theta Sketches - Count Distinct Elements w/ Set Operations (Sometimes less fast)
* KLL Floats Sketches - Compute Quantiles / Ranks (Less precision, more efficient)
* KLL Doubles Sketches - Compute Quantiles / Ranks (More precision, less efficient)
* String Items Sketches - Estimate frequency of String entries
* Double Items Sketches - Estimate frequency of Double / Real / Float entries
* Long Items Sketches - Estimate frequency of Long / Int entries

## DataSketches UDF Documentation

- [HLL Sketch](./datasketches-udfs/hll-sketch)
- [Theta Sketch](./datasketches-udfs/theta-sketch)
- [KLL Floats Sketch](./datasketches-udfs/kll-floats-sketch)
- [KLL Doubles Sketch](./datasketches-udfs/kll-doubles-sketch)
- [String Items Sketch](./datasketches-udfs/string-items-sketch)
- [Double Items Sketch](./datasketches-udfs/double-items-sketch)
- [Long Items Sketch](./datasketches-udfs/long-items-sketch)