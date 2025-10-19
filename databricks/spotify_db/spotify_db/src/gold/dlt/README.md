# Nouveau pipeline 2025-10-18 17:04

This folder defines all source code for the 'Nouveau pipeline 2025-10-18 17:04' pipeline:

- `explorations`: Ad-hoc notebooks used to explore the data processed by this pipeline.
- `transformations`: All dataset definitions and transformations.
- `utilities`: Utility functions and Python modules used in this pipeline.

## Getting Started

To get started, go to the `transformations` folder -- most of the relevant source code lives there:

* By convention, every dataset under `transformations` is in a separate file.
* Take a look at the sample under "sample_trips_nouveau_pipeline_2025_10_18_17_04.py" to get familiar with the syntax.
  Read more about the syntax at https://docs.databricks.com/dlt/python-ref.html.
* Use `Run file` to run and preview a single transformation.
* Use `Run pipeline` to run _all_ transformations in the entire pipeline.
* Use `+ Add` in the file browser to add a new data set definition.
* Use `Schedule` to run the pipeline on a schedule!

For more tutorials and reference material, see https://docs.databricks.com/dlt.