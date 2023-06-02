### Aggregations and Grouping

**Are wide transformations**

when one or more input partitions, contributes to one or more output partitions
with shuffles, that means the data is being moved between diff nodes in spark cluster
shuffling it's an expensive operation.

IF YOU HAVE PIPELINES THAT YOU ALSO WANT TO BE REALLY FAST, BE CAREFUL WHEN YOU ARE USING DATA AGGREGATIONS AND
GROUPING.
OFTEN IN THE CASE, NOT ALWAYS BUT OFTEN IT'S THE CASE THAT IT WOULD BE BEST TO DO DATA AGGREGATIONS AND GROUPING
AT THE END OF THE PROCESSING
