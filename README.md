# CSV to BigQuery Dataflow Stream

This is a demo program which reads CSV file(s) from a GCS folder continuously, transforms the data, persists into BigQuery. 

## Steps used by the pipeline 
1. Collect files for processing continuously - once every 10 seconds.
2. Match all the file names against the files and compile metadata
3. Read all the file contents line-by-line and compile a map of fileName and line. Move files to archive.
    1. Process each line in accordance with the custom logic and construct a BigQuery TableRow per line
    2. Persist the records into BigQuery table
    3. Extract rows that failed to insert into BigQuery along with the error
    4. Persist the failed rows along with error message into an error file in GCS

## Prerequisites 
Pleas ensure you have the prerequisite defined here : https://cloud.google.com/dataflow/docs/quickstarts/quickstart-java-maven#before-you-begin

## Usage 
* Compile the source code 
* Deploy the job into Dataflow using 
```sh
mvn compile exec:java \
 -Dexec.mainClass=com.dataflow.demo.templates.CsvToBigQueryStreaming \
 -Dexec.args="--runner=DataflowRunner \
              --project=GCP_PROJECT_NAME \
              --inputFilePattern=gs://SOURCE_BUCKET/*.csv \
              --delimiter=, \
              --JSONPath=gs://CONFIG_BUCKET/bq-table-schema.json  \
              --outputTable=[PROJECT_NAME]:[DATASET].[TABLE_NAME]  \
              --archiveFolder=gs://ARCHIVE_BUCKET  \
              --errorFolder=gs://ERROR_BUCKET \
              --bigQueryLoadingTemporaryDirectory=gs://TEMP_BUCKET"
```  
* [Sample BigQuery schema](config/bq-table-schema.json)
* [Sample Input Data](config/transactions.csv)

## Comments
* This is a demo application. Please ensure you test all relavant use cases and implement test cases. 
* This will be re-written using `ContextualTextIO` to manage CSV and archival better. Watch this space!