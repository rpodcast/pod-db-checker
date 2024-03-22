## Podcast Index Database Analysis

This repository contains analysis pipelines supporting the [PodcastIndex Database Dashboard](https://rpodcast.github.io/pod-db-dash/):

* Identify potential duplicate podcast entries in the database using the record linkage method.
* Assess the quality of the database records with specific assessment criteria

Each of these analyses are performed weekly after the Podcast Index SQLite database is updated.

## Duplicate Analysis

The methodology used to identify candidate duplicates is a technique called [record linkage](https://rpubs.com/ahmademad/RecordLinkage). At a high level, record linkage evaluates possible pairwise combinations of records from two data sets (or in the case of de-duplication, a single data set compared to itself) and determines if a given pair are likely to originate from the same entity. For a data set the size of the Podcast Index, it would be next to impossible to perform the analysis on all pairwise combinations of records. Next we describe the techniques for pruning the possible record combinations and the criteria used to categorize a given pair as a possible duplicate. Within R, we leverage the [`{reclin2}`](https://github.com/djvanderlaan/reclin2) package that strikes a nice balance between performance and logical workflow for performing probabilistic record linkage and de-duplication.

### Reducing Candidate Pairs

To reduce candidate record pairs supplied to the record linkage analyses, we apply a technique called **blocking**, which requires a pair of records to agree on one or multiple variables before it can be moved to further analysis. For this analysis, we are using the **title** and **content hash** variables as the blocking variables. 

### Comparing Pairs

With the candidate pairs available, the next step is to derive a similarity score between the records in a given pair based on a set of variables common between the records. Based on advice from the Pod Sage Dave Jones, we use the following variables for comparison:

* URL
* Newest Enclosure URL
* Image URL

The statistical method used to derive the similarity score is the [Jaro-Winkler distance](https://en.wikipedia.org/wiki/Jaro%E2%80%93Winkler_distance) metric, which is a great fit for the URL variables. The metric produces a score ranging from 0 (no match in any of the string characters) to 1 (perfect match between the strings). The algorithm can be customized with a threshold value that gives a cutoff for determining if the two strings are a likely match. For this analysis we use a threshold of 0.95, but this is up for discussion as there is a tradeoff between a threshold value and the number of candidate duplicate groupings identified. This is a subject that requires further attention going forward.

With the Jaro-WInkler distance score calculated, only the records with a score of 0.95 or above will be retained for further evaluation.

### Derive Duplicate Groups

Once the candidate pairs are pruned with the threshold cutoff, the last step is to organize the potential duplicate records into groups. The dashboard presents each of these groups with the ability to drill down within each group and inspect the records that were considered duplicates.

## Data Quality Assessments

The following assessments are performed to evaluate the quality of the Podcast Index:

* Non-missing podcast GUID values
* Unique Podcast GUID values
* Non-missing podcast content hashes
* Non-missing newest enclosure duration
* Valid explicit flag values (0, 1)
* Valid last HTTP status values
* Valid newest enclosure duration (above 0)
* Unique content hash values for all podcast entries in each host
* Unique podcast title and image URL combinations
* Unique podcast content hash, title, and image URL combinations

## Tech Stack

Much like the ethos behind podcasting 2.0, the PodcastIndex Dashboard is proudly built on the foundations of open-source, using the [R](https://r-project.org) project for statistical computing with the following amazing packages:

+ [`{reclin2}`](https://github.com/djvanderlaan/reclin2): Record linkage toolkit for R.
+ [`{pointblank}`](https://rstudio.github.io/pointblank/): Data quality assessment and metadata reporting for data frames and database tables. 
+ [`{DBI}`](https://dbi.r-dbi.org/): A database interface (DBI) definition for communication between R and RDBMSs.
+ [`{RSQLite}`](https://rsqlite.r-dbi.org/): R interface to SQLite.
+ [`{dplyr}`](https://dplyr.tidyverse.org/): A grammar of data manipulation.
+ [`{dbplyr}`](https://dbplyr.tidyverse.org/): Database (DBI) backend for `{dplyr}`.
+ [`{tidyr}`](https://tidyr.tidyverse.org/): TIdy messy data.
+ [`{logger}`](https://daroczig.github.io/logger/): A lightweight, modern and flexible, log4j and futile.logger inspired logging utility for R.
+ [`{arrow}`](https://github.com/apache/arrow/tree/main/r): R binding to the Apache Arrow C++ library.
+ [`{s3fs}`](https://dyfanjones.github.io/s3fs/): Access Amazon Web Service S3 as if it were a file system.
+ [`{anytime}`](https://eddelbuettel.github.io/anytime): Anything to POSIXct or Date converter.

The scripts for performing the schedule analyses are the following:

* [`duplicate_runner.R`](https://github.com/rpodcast/pod-db-checker/blob/main/duplicate_runner.R): Performs duplication analysis and necessary data pre-processing.
* [`pointblank_runner.R`](https://github.com/rpodcast/pod-db-checker/blob/main/pointblank_runner.R): Execute data quality checks with the `{pointblank}` package.

Each of these scripts are run weekly via GitHub actions.
