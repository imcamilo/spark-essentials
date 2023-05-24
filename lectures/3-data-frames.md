### dataframes

We can think of data frames of some kind 
of distributed spreadsheets with rows and columns.

So we can think of data frames as a table which is 
split between multiple nodes and spark clusters.

The information that each spark node receives is the
schema of the data frame and a few of the rows that 
compose the data frame

#### More Technical Terms

- #### A dataframe are distributed collections of rows conforming to a schema.
  - A schema is a list of column names and column types.
  - These types are known by spark when them are used, not a compiled time.
  - Arbitrary number of columns.
  - All rows have the same structure.

- #### Need to be distributed
  - Data is too big for a single computer.
  - Too long to process the entire data in a single CPU.
  
- #### Partitioning
  - Splits data into files, distributed (shared) between nodes in the cluster.
  - Will impact the processing parallelism of the data. (more partitions means more parallelism).

- #### Immutable
  - Cant be changed once created
  - Can creat other DF via transformations

- #### Transformation
  - **Narrow**. One input partition contributes to at most one output partition (e.g. **map**). The map will change the DF row by row, but the partitioning will not change.
  - **Wide**. Input partitions (one or more) create many output partitions (e.g. **sort**)

- #### Shuffle

  Data exchange between cluster nodes.

  - Occurs in **wide transformations**
  - It's a massive performance topic. Can impact you jobs in orders of magnitude.
