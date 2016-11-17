Definition

Windows are used to divide a (potentially) infinite Stream into finite slices based on the timestamps of elements or other criteria. This division is required when working with infinite streams of data and performing transformations that aggregate elements.
This project aims to analyse the impact of window size to latency with different streaming systems: Apache Flink [1], Apache Spark [2], Apache Storm (Trident API) [3].
Experiments are expected to conduct with different

cluster size
workload (input data speed)
window length and slide
window types: partitioned and non-partitioned
batch size (for Spark and Trident).
Deliverables

The implementation of benchmarks of 3 systems indicated above.
A report which contains design of a solution and extensive analysis of experimental results.



References

[1] https://flink.apache.org
[2] http://spark.apache.org
[3] http://storm.apache.org/releases/1.0.1/Trident-API-Overview.html

by Patrick Lehmann
