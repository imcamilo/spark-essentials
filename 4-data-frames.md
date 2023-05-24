### dataframes

- #### Lazy Evaluation
  - Spark waits until the last moment to execute the DF transformations.

- #### Planning
  - Spark compiles the DF transformations and dependencies into a graph before running any code.
  - Logical Plan: DF dependency graph + narrow/wide transformations sequence.
  - Physical Plan: Optimize the sequence of steps for nodes in the cluster.
  - Optimizations

- #### Transformations vs Actions
  - Transformation describes how a new DF are obtained.
  - Actions actually start executing Spark code. When we call show, count, trigger spark evaluate those DF.
