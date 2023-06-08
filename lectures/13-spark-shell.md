### Spark Shell

Inside the spark shell:

I can go to /spark/bin/ and ./spark-shell.

```markdown
Spark context Web UI available at http://8452d6985c9b:4040
Spark context available as 'sc' (master = local[*], app id = local-1686170859975).
Spark session available as 'spark'.
Welcome to
____              __
/ __/__  ___ _____/ /__
_\ \/ _ \/ _ `/ __/  '_/
/___/ .__/\_,_/_/ /_/\_\ version 3.2.1
/_/

Using Scala version 2.12.15 (OpenJDK 64-Bit Server VM, Java 1.8.0_372)
Type in expressions to have them evaluated.
Type :help for more information.
```

Spark shell offers a web application in localhost:4040.

- Jobs tabs: describes jobs that are running in the cluster.
- Stages: describes stages for individual jobs.
- Storage: descrbies driver and executors.
- Environment: All properties and configuration that spark is running.
- Executors: Describes the data related to the nodes in our spark cluster, util for debbuging and optimization purposes.

### Terminology

We have some divisions for the jobs.

#### A job has stages and, a stages has tasks.

**Stages**: A set of computations between shuffles. (Exchange data between spark nodes).

**Task**: A unit of computation, per partition.

**DAG (Directed Acyclic Graph)**:  Graph of RDD dependencies.


Physical plans (and planning behind the scenes) are an advance topic.


