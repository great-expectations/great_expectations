![PyPI - Python Version](https://img.shields.io/pypi/pyversions/DataProfiler)
![GitHub](https://img.shields.io/github/license/CapitalOne/DataProfiler)
![GitHub last commit](https://img.shields.io/github/last-commit/CapitalOne/DataProfiler)

# Data Profiler | What's in your data?

The DataProfiler is a Python library designed to make data analysis, monitoring and **sensitive data detection** easy.

Loading **Data** with a single command, the library automatically formats & loads files into a DataFrame. **Profiling** the Data, the library identifies the schema, statistics, entities (PII / NPI) and more. Data Profiles can then be used in downstream applications or reports.

Getting started only takes a few lines of code ([example csv](https://raw.githubusercontent.com/capitalone/DataProfiler/main/dataprofiler/tests/data/csv/aws_honeypot_marx_geo.csv)):

```python
import json
from dataprofiler import Data, Profiler

data = Data("your_file.csv") # Auto-Detect & Load: CSV, AVRO, Parquet, JSON, Text, URL

print(data.data.head(5)) # Access data directly via a compatible Pandas DataFrame

profile = Profiler(data) # Calculate Statistics, Entity Recognition, etc

readable_report = profile.report(report_options={"output_format": "compact"})

print(json.dumps(readable_report, indent=4))
```
Note: The Data Profiler comes with a pre-trained deep learning model, used to efficiently identify **sensitive data** (PII / NPI). If desired, it's easy to add new entities to the existing pre-trained model or insert an entire new pipeline for entity recognition.

For API documentation, visit the [documentation page](https://capitalone.github.io/DataProfiler/).

If you have suggestions or find a bug, [please open an issue](https://github.com/capitalone/dataprofiler/issues/new/choose).

------------------

# Install

**To install the full package from pypi**: `pip install DataProfiler[full]`

If you want to install the ml dependencies without generating reports use `DataProfiler[ml]`

If the ML requirements are too strict (say, you don't want to install tensorflow), you can install a slimmer package with `DataProfiler[reports]`. The slimmer package disables the default sensitive data detection / entity recognition (labler)

Install from pypi: `pip install DataProfiler`


------------------

# What is a Data Profile?

In the case of this library, a data profile is a dictionary containing statistics and predictions about the underlying dataset. There are "global statistics" or `global_stats`, which contain dataset level data and there are "column/row level statistics" or `data_stats` (each column is a new key-value entry).

The format for a structured profile is below:

```
"global_stats": {
    "samples_used": int,
    "column_count": int,
    "row_count": int,
    "row_has_null_ratio": float,
    "row_is_null_ratio": float,
    "unique_row_ratio": float,
    "duplicate_row_count": int,
    "file_type": string,
    "encoding": string,
    "correlation_matrix": list[list[int]], (*)
    "chi2_matrix": list[list[float]],
    "profile_schema": {
        string: list[int]
    },
    "times": dict[string, float],
},
"data_stats": [
    {
        "column_name": string,
        "data_type": string,
        "data_label": string,
        "categorical": bool,
        "order": string,
        "samples": list[str],
        "statistics": {
            "sample_size": int,
            "null_count": int,
            "null_types": list[string],
            "null_types_index": {
                string: list[int]
            },
            "data_type_representation": dict[string, float],
            "min": [null, float, str],
            "max": [null, float, str],
            "mode": float,
            "median": float,
            "median_absolute_deviation": float,
            "sum": float,
            "mean": float,
            "variance": float,
            "stddev": float,
            "skewness": float,
            "kurtosis": float,
            "num_zeros": int,
            "num_negatives": int,
            "histogram": {
                "bin_counts": list[int],
                "bin_edges": list[float],
            },
            "quantiles": {
                int: float
            },
            "vocab": list[char],
            "avg_predictions": dict[string, float],
            "data_label_representation": dict[string, float],
            "categories": list[str],
            "unique_count": int,
            "unique_ratio": float,
            "categorical_count": dict[string, int],
            "gini_impurity": float,
            "unalikeability": float,
            "precision": {
                'min': int,
                'max': int,
                'mean': float,
                'var': float,
                'std': float,
                'sample_size': int,
                'margin_of_error': float,
                'confidence_level': float
            },
            "times": dict[string, float],
            "format": string
        }
    }
]
```
(*) Currently the correlation matrix update is toggled off. It will be reset in a later update. Users can still use it as desired with the is_enable option set to True.

The format for an unstructured profile is below:
```
"global_stats": {
    "samples_used": int,
    "empty_line_count": int,
    "file_type": string,
    "encoding": string,
    "memory_size": float, # in MB
    "times": dict[string, float],
},
"data_stats": {
    "data_label": {
        "entity_counts": {
            "word_level": dict[string, int],
            "true_char_level": dict[string, int],
            "postprocess_char_level": dict[string, int]
        },
        "entity_percentages": {
            "word_level": dict[string, float],
            "true_char_level": dict[string, float],
            "postprocess_char_level": dict[string, float]
        },
        "times": dict[string, float]
    },
    "statistics": {
        "vocab": list[char],
        "vocab_count": dict[string, int],
        "words": list[string],
        "word_count": dict[string, int],
        "times": dict[string, float]
    }
}
```
# Profile Statistic Descriptions

### Structured Profile

#### global_stats:

* `samples_used` - number of input data samples used to generate this profile
* `column_count` - the number of columns contained in the input dataset
* `row_count` - the number of rows contained in the input dataset
* `row_has_null_ratio` - the proportion of rows that contain at least one null value to the total number of rows
* `row_is_null_ratio` - the proportion of rows that are fully comprised of null values (null rows) to the total number of rows
* `unique_row_ratio` - the proportion of distinct rows in the input dataset to the total number of rows
* `duplicate_row_count` - the number of rows that occur more than once in the input dataset
* `file_type` - the format of the file containing the input dataset (ex: .csv)
* `encoding` - the encoding of the file containing the input dataset (ex: UTF-8)
* `correlation_matrix` - matrix of shape `column_count` x `column_count` containing the correlation coefficients between each column in the dataset
* `chi2_matrix` - matrix of shape `column_count` x `column_count` containing the chi-square statistics between each column in the dataset
* `profile_schema` - a description of the format of the input dataset labeling each column and its index in the dataset
    * `string` - the label of the column in question and its index in the profile schema
* `times` - the duration of time it took to generate the global statistics for this dataset in milliseconds

#### data_stats:

* `column_name` - the label/title of this column in the input dataset
* `data_type` - the primitive python data type that is contained within this column
* `data_label` - the label/entity of the data in this column as determined by the Labeler component
* `categorical` - ‘true’ if this column contains categorical data
* `order` - the way in which the data in this column is ordered, if any, otherwise “random”
* `samples` - a small subset of data entries from this column
* `statistics` - statistical information on the column
    * `sample_size` - number of input data samples used to generate this profile
    * `null_count` - the number of null entries in the sample
    * `null_types` - a list of the different null types present within this sample
    * `null_types_index` - a dict containing each null type and a respective list of the indicies that it is present within this sample
    * `data_type_representation` - the percentage of samples used identifying as each data_type
    * `min` - minimum value in the sample
    * `max` - maximum value in the sample
    * `mode` - mode of the entries in the sample
    * `median` - median of the entries in the sample
    * `median_absolute_deviation` - the median absolute deviation of the entries in the sample
    * `sum` - the total of all sampled values from the column
    * `mean` - the average of all entries in the sample
    * `variance` - the variance of all entries in the sample
    * `stddev` - the standard deviation of all entries in the sample
    * `skewness` - the statistical skewness of all entries in the sample
    * `kurtosis` - the statistical kurtosis of all entries in the sample
    * `num_zeros` - the number of entries in this sample that have the value 0
    * `num_negatives` - the number of entries in this sample that have a value less than 0
    * `histogram` - contains histogram relevant information
        * `bin_counts` - the number of entries within each bin
        * `bin_edges` - the thresholds of each bin
    * `quantiles` - the value at each percentile in the order they are listed based on the entries in the sample
    * `vocab` - a list of the characters used within the entries in this sample
    * `avg_predictions` - average of the data label prediction confidences across all data points sampled
    * `categories` - a list of each distinct category within the sample if `categorial` = 'true'
    * `unique_count` - the number of distinct entries in the sample
    * `unique_ratio` - the proportion of the number of distinct entries in the sample to the total number of entries in the sample
    * `categorical_count` - number of entries sampled for each category if `categorical` = 'true'
    * `gini_impurity` - measure of how often a randomly chosen element from the set would be incorrectly labeled if it was randomly labeled according to the distribution of labels in the subset
    * `unalikeability` - a value denoting how frequently entries differ from one another within the sample
    * `precision` - a dict of statistics with respect to the number of digits in a number for each sample
    * `times` - the duration of time it took to generate this sample's statistics in milliseconds
    * `format` - list of possible datetime formats

### Unstructured Profile

#### global_stats:

* `samples_used` - number of input data samples used to generate this profile
* `empty_line_count` - the number of empty lines in the input data
* `file_type` - the file type of the input data (ex: .txt)
* `encoding` - file encoding of the input data file (ex: UTF-8)
* `memory_size` - size of the input data in MB
* `times` - duration of time it took to generate this profile in milliseconds

#### data_stats:

* `data_label` - labels and statistics on the labels of the input data
    * `entity_counts` - the number of times a specific label or entity appears inside the input data
        * `word_level` - the number of words counted within each label or entity
        * `true_char_level` - the number of characters counted within each label or entity as determined by the model
        * `postprocess_char_level` - the number of characters counted within each label or entity as determined by the postprocessor
    * `entity_percentages` - the percentages of each label or entity within the input data
        * `word_level` - the percentage of words in the input data that are contained within each label or entity
        * `true_char_level` - the percentage of characters in the input data that are contained within each label or entity as determined by the model
        * `postprocess_char_level` - the percentage of characters in the input data that are contained within each label or entity as determined by the postprocessor
    * `times` - the duration of time it took for the data labeler to predict on the data
* `statistics` - statistics of the input data
    * `vocab` - a list of each character in the input data
    * `vocab_count` - the number of occurrences of each distinct character in the input data
    * `words` - a list of each word in the input data
    * `word_count` - the number of occurrences of each distinct word in the input data
    * `times` - the duration of time it took to generate the vocab and words statistics in milliseconds

# Support

### Supported Data Formats

* Any delimited file (CSV, TSV, etc.)
* JSON object
* Avro file
* Parquet file
* Text file
* Pandas DataFrame
* A URL that points to one of the supported file types above

### Data Types

*Data Types* are determined at the column level for structured data

* Int
* Float
* String
* DateTime

### Data Labels

*Data Labels* are determined per cell for structured data (column/row when the *profiler* is used) or at the character level for unstructured data.

* UNKNOWN
* ADDRESS
* BAN (bank account number, 10-18 digits)
* CREDIT_CARD
* EMAIL_ADDRESS
* UUID
* HASH_OR_KEY (md5, sha1, sha256, random hash, etc.)
* IPV4
* IPV6
* MAC_ADDRESS
* PERSON
* PHONE_NUMBER
* SSN
* URL
* US_STATE
* DRIVERS_LICENSE
* DATE
* TIME
* DATETIME
* INTEGER
* FLOAT
* QUANTITY
* ORDINAL

# Get Started

### Load a File

The Data Profiler can profile the following data/file types:

* CSV file (or any delimited file)
* JSON object
* Avro file
* Parquet file
* Text file
* Pandas DataFrame
* A URL that points to one of the supported file types above

The profiler should automatically identify the file type and load the data into a `Data Class`.

Along with other attributtes the `Data class` enables data to be accessed via a valid Pandas DataFrame.

```python
# Load a csv file, return a CSVData object
csv_data = Data('your_file.csv')

# Print the first 10 rows of the csv file
print(csv_data.data.head(10))

# Load a parquet file, return a ParquetData object
parquet_data = Data('your_file.parquet')

# Sort the data by the name column
parquet_data.data.sort_values(by='name', inplace=True)

# Print the sorted first 10 rows of the parquet data
print(parquet_data.data.head(10))

# Load a json file from a URL, return a JSONData object
json_data = Data('https://github.com/capitalone/DataProfiler/blob/main/dataprofiler/tests/data/json/iris-utf-8.json')
```

If the file type is not automatically identified (rare), you can specify them
specifically, see section [Specifying a Filetype or Delimiter](#specifying-a-filetype-or-delimiter).

### Profile a File

Example uses a CSV file for example, but CSV, JSON, Avro, Parquet or Text should also work.

```python
import json
from dataprofiler import Data, Profiler

# Load file (CSV should be automatically identified)
data = Data("your_file.csv")

# Profile the dataset
profile = Profiler(data)

# Generate a report and use json to prettify.
report  = profile.report(report_options={"output_format": "pretty"})

# Print the report
print(json.dumps(report, indent=4))
```

### Updating Profiles

Currently, the data profiler is equipped to update its profile in batches.

```python
import json
from dataprofiler import Data, Profiler

# Load and profile a CSV file
data = Data("your_file.csv")
profile = Profiler(data)

# Update the profile with new data:
new_data = Data("new_data.csv")
profile.update_profile(new_data)

# Print the report using json to prettify.
report  = profile.report(report_options={"output_format": "pretty"})
print(json.dumps(report, indent=4))
```

Note that if the data you update the profile with contains integer indices that overlap with the indices on data originally profiled, when null rows are calculated the indices will be "shifted" to uninhabited values so that null counts and ratios are still accurate.

### Merging Profiles

If you have two files with the same schema (but different data), it is possible to merge the two profiles together via an addition operator.

This also enables profiles to be determined in a distributed manner.

```python
import json
from dataprofiler import Data, Profiler

# Load a CSV file with a schema
data1 = Data("file_a.csv")
profile1 = Profiler(data1)

# Load another CSV file with the same schema
data2 = Data("file_b.csv")
profile2 = Profiler(data2)

profile3 = profile1 + profile2

# Print the report using json to prettify.
report  = profile3.report(report_options={"output_format": "pretty"})
print(json.dumps(report, indent=4))
```

Note that if merged profiles had overlapping integer indices, when null rows are calculated the indices will be "shifted" to uninhabited values so that null counts and ratios are still accurate.

### Profiler Differences
For finding the change between profiles with the same schema we can utilize the
profile's `diff` function. The diff will provide overall file and sampling
differences as well as detailed differences of the data's statistics. For
example, numerical columns have a t-test applied to evaluate similarity.
More information is described in the Profiler section of the [Github Pages](
https://capitalone.github.io/DataProfiler/).

Create the difference report like this:
```python
import json
import dataprofiler as dp

# Load a CSV file
data1 = dp.Data("file_a.csv")
profile1 = dp.Profiler(data1)

# Load another CSV file
data2 = dp.Data("file_b.csv")
profile2 = dp.Profiler(data2)

diff_report = profile1.diff(profile2)
print(json.dumps(diff_report, indent=4))
```

### Profile a Pandas DataFrame
```python
import pandas as pd
import dataprofiler as dp
import json

my_dataframe = pd.DataFrame([[1, 2.0],[1, 2.2],[-1, 3]])
profile = dp.Profiler(my_dataframe)

# print the report using json to prettify.
report = profile.report(report_options={"output_format": "pretty"})
print(json.dumps(report, indent=4))

# read a specified column, in this case it is labeled 0:
print(json.dumps(report["data_stats"][0], indent=4))
```

### Unstructured profiler
In addition to the structured profiler, DataProfiler provides unstructured profiling for the TextData object or string. The unstructured profiler also works with list[string], pd.Series(string) or pd.DataFrame(string) given profiler_type option specified as `unstructured`. Below is an example of the unstructured profiler with a text file.
```python
import dataprofiler as dp
import json

my_text = dp.Data('text_file.txt')
profile = dp.Profiler(my_text)

# print the report using json to prettify.
report = profile.report(report_options={"output_format": "pretty"})
print(json.dumps(report, indent=4))
```

Another example of the unstructured profiler with pd.Series of strings is given as below, with the profiler option `profiler_type='unstructured'`
```python
import dataprofiler as dp
import pandas as pd
import json

text_data = pd.Series(['first string', 'second string'])
profile = dp.Profiler(text_data, profiler_type='unstructured')

# print the report using json to prettify.
report = profile.report(report_options={"output_format": "pretty"})
print(json.dumps(report, indent=4))
```
**Visit the [documentation page](https://capitalone.github.io/DataProfiler/) for additional Examples and API details**


# References
```
Sensitive Data Detection with High-Throughput Neural Network Models for Financial Institutions
Authors: Anh Truong, Austin Walters, Jeremy Goodsitt
2020 https://arxiv.org/abs/2012.09597
The AAAI-21 Workshop on Knowledge Discovery from Unstructured Data in Financial Services
```

GX Integration Author: Taylor Turner ([taylorfturner](https://github.com/taylorfturner))
