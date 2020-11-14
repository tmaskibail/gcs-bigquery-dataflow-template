package com.dataflow.demo.templates.transform;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

public interface CsvToBigQueryStreamingOptions extends DataflowPipelineOptions {
    @Validation.Required
    @Description("The GCS location of the text you'd like to process")
    @Default.String("gs://gcs-bucket/source")
    ValueProvider<String> getInputFilePattern();

    void setInputFilePattern(ValueProvider<String> value);

    @Description("Delimiter to use when parsing the rows")
    @Default.String(",")
    ValueProvider<String> getDelimiter();

    void setDelimiter(ValueProvider<String> value);

    @Validation.Required
    @Description("JSON file with BigQuery Schema description")
    ValueProvider<String> getJSONPath();

    void setJSONPath(ValueProvider<String> value);

    @Description("Output BigQuery table to write to")
    @Validation.Required
    @Default.String("[project_id]:[dataset_id].[table_id]")
    ValueProvider<String> getOutputTable();

    void setOutputTable(ValueProvider<String> value);

    @Validation.Required
    @Description("Path of the archive folder")
    @Default.String("gs://gcs-bucket/archive")
    ValueProvider<String> getArchiveFolder();

    void setArchiveFolder(ValueProvider<String> value);

    @Validation.Required
    @Description("Path where error file will be written")
    @Default.String("gs://gcs-bucket/error")
    ValueProvider<String> getErrorFolder();

    void setErrorFolder(ValueProvider<String> value);

    @Validation.Required
    @Description("Temporary directory for BigQuery loading process")
    ValueProvider<String> getBigQueryLoadingTemporaryDirectory();

    void setBigQueryLoadingTemporaryDirectory(ValueProvider<String> directory);
}
