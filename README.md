### Analysing COVID-19 Data with Spark

In this assignment, you will process COVID-19 data from the U.S. CMS website, performing some simple processing. The assignment can be satisfied by supplying either a standalone application, `pyspark` shell or `spark-shell` code, or a Jupyter notebook. You will be asked to walk through your code "live", explain what it does, and explain your implementation choices. By "live", we mean you should be able to run your code during the walkthrough; if for some reason this will not be possible, please let us know why before we meet to walk through your solution. Anywhere that the term `DataFrame` is used, Scala users can use a `Dataset` if they wish.

The content in this assignment would take an average of three hours, but do not spend more than two hours working on it. The goal is to ensure you clearly understand the requirements, not to complete this assignment in its entirety. Please reach out to your Cigna rep with questions; they will be passed to the customer and answered the same day.

This file is in Markdown in case you want to use it as the shell of a Jupyter notebook, but there is no requirement to use it other than as a source for your assignment instructions.

#### The data set:

The data is included with this assignment and is named `Medicare_COVID_19_Data_Snapshot_January2021_20210126.csv`. You don't need to download any data, but if you'd like to explore the CMS website which is the source of the data, visit [Preliminary Medicare COVID-19 Data Snapshot](https://www.cms.gov/research-statistics-data-systems/preliminary-medicare-covid-19-data-snapshot).

#### Data exploration:

This data file is similar to a lot of other data files posted by the government, in that it contains records of multiple schemas in a single file. Think "schemaless DynamoDB." While this approach saves the generating organization the trouble of producing (e.g.) 20 files of 50 lines each, it does create a little extra work for the data analyst/engineer. We'll start by exploring the data and writing some code to sift out these different record types.

In your `Spark` application:

* Read the data file into a `Spark` `DataFrame`, performing cleanup on the data file if necessary
* Produce a count of all rows
* Generate and output a small `DataFrame` containing all the "overall case" counts and their corresponding dates
* Further inspect the data (_this might be just as easy in `vi`, by the way!_) and select the column(s) in the data which distinguishes different types of records. 
Describe very briefly your set of "composite primary key" columns.
* Generate and output a `DataFrame` containing the list of all unique record types and the record count for each type

#### Summary reports and metrics:

##### Analyze a specific date:

* Choose a single `Claims_Thru_Dt` with `Measure_Level` equal to `COVID-19 Cases by State` and `Measure_Unit` equal to `Beneficiary Count`
* For that date, retrieve the `Value` for the `Overall` COVID-19 Cases
* Sum the counts for every `COVID-19 Cases by State` `Measure_Element` that is an actual US state or territory
* Verify that the Overall total case count, minus the aggregation of all state/territory counts, is equal to the Missing Data count


##### Create a summary over all available dates:

This section is in two parts:

* Part 1 involves creating an output JSON data structure for a single state
* Part 2 involves creating an output JSON data structure for all US states and territories

###### Single-state summary:

* Choose the `Measure_Level` equal to `COVID-19 Cases by State`
* Create a data structure which produces the case data for Alabama, for each available date
* Design and generate a nested JSON data structure which allows retrieval of a individual claims counts per date for Alabama

  For example, the structure could look like the following:

```json
{ "Alabama": [
  {"Claims_Thru_Dt": "10/26/2020", "Count": "56285.0"},
  {"Claims_Thru_Dt": "11/26/2020", "Count": "46285.0"},
  {"Claims_Thru_Dt": "12/26/2020", "Count": "36285.0"},
  ...
]}
```

###### All states and territories:

* Again, choose the `Measure_Level` equal to `COVID-19 Cases by State`
* Create a data structure which produces the case data for each state, for each available date
* Design and generate a nested JSON data structure which allows retrieval of a time-series of case data per state

  For example, the structure should look like the following:

```json
{"COVID-19 Cases by State": [
  { "Alabama": [
    {"Claims_Thru_Dt": "10/26/2020", "Count": "56285.0"},
    {"Claims_Thru_Dt": "11/26/2020", "Count": "46285.0"},
    {"Claims_Thru_Dt": "12/26/2020", "Count": "36285.0"}
  ]},
  { "Alaska": [
    {"Claims_Thru_Dt": "10/26/2020", "Count": "5285.0"},
    {"Claims_Thru_Dt": "11/26/2020", "Count": "4285.0"},
    {"Claims_Thru_Dt": "12/26/2020", "Count": "3285.0"}
  ]},
  ...
 ]
}
```

* Persist the JSON data structure to a local file
* Ensure the document is legally-constructed JSON (e.g. by formatting it in a website such as [jsonformatter.org](https://jsonformatter.org/)).


