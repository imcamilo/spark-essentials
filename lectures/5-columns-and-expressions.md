### columns and expressions

Operate on Data Frame Columns And Obtain new DataFrames with expressions

The goal is to operate on DF columns and obtain new DF by what's called **Projections of DF**.

- How to operate with DF with complex expressions.

## Columns

Are special objects that allow us to obtain new DF out of some source data
frames by processing values inside.

### Select

- Selecting

    Remember that the DF are actually split in partitions, between nodes in the cluster.
    When I select 2 columns or any number of columns from a dataframe, those columns are being selected on every partition on every node when the DF resides. 
    
- And after select
  I will obtain a new DF with those column that I wanted, and it will be reflected in every node in the cluster.

**That's narrow transformation**

This is an example of what we earlier called a **narrow transformation**.
That means that every input partition in the original data frame has exactly one 
corresponding output partition in the resulting data frame.

