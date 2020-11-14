/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.dataflow.demo.templates;

import com.dataflow.demo.templates.transform.CsvToBigQueryStreamingOptions;
import com.dataflow.demo.templates.transform.CsvToJsonTransform;
import com.dataflow.demo.templates.transform.ParseAndArchiveRowTransform;
import com.google.api.client.json.JsonFactory;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.text.SimpleDateFormat;
import java.util.Date;

public class CsvToBigQueryStreaming {

    private static final Logger LOG = LoggerFactory.getLogger(CsvToBigQueryStreaming.class);
    /**
     * Default interval for polling files in GCS.
     */
    private static final Duration DEFAULT_POLL_INTERVAL = Duration.standardSeconds(10);
    private static final JsonFactory JSON_FACTORY = Transport.getJsonFactory();

    /**
     * Main entry point for executing the pipeline. This will run the pipeline asynchronously. If
     * blocking execution is required, use the {@link
     * CsvToBigQueryStreaming#run(CsvToBigQueryStreamingOptions)} method to start the pipeline
     * and invoke {@code result.waitUntilFinish()} on the {@link PipelineResult}
     *
     * @param args The command-line arguments to the pipeline.
     */
    public static void main(String[] args) {
        // Parse the user options passed from the command-line
        CsvToBigQueryStreamingOptions options =
                PipelineOptionsFactory.fromArgs(args)
                        .withValidation()
                        .as(CsvToBigQueryStreamingOptions.class);
        run(options);
    }

    /**
     * Runs the pipeline with the supplied options.
     *
     * @param options The execution parameters to the pipeline.
     * @return The result of the pipeline execution.
     */
    public static PipelineResult run(CsvToBigQueryStreamingOptions options) {

        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);

        /*
         * Steps:
         *  1) Collect files for processing continuously - once every 10 seconds.
         *  2) Match all the file names against the files and compile metadata
         *  3) Read all rows line-by-line per file and move file to archive,
         *  3.1) Process each line in accordance with the custom logic and construct a BigQuery TableRow per line
         *  3.2) Persist the records into BigQuery table
         *  3.3) Extract rows that failed to insert into BigQuery along with the error
         *  3.4) Persist the failed rows along with error message into an error file in GCS
         */


        PCollection<KV<String, String>> dataToBeProcessed =
                pipeline
                        // 1) Collect files for processing continuously - once every 10 seconds.
                        .apply("ReadFromSource",
                                FileIO.match()
                                        .filepattern(options.getInputFilePattern())
                                        .continuously(DEFAULT_POLL_INTERVAL, Watch.Growth.never()))

                        // 2) Match all the file names against the files and compile metadata
                        .apply("CollectAllMatchingFiles", FileIO.readMatches())

                        // 3) Read all the file contents line-by-line and compile a map of fileName and line
                        .apply("ParseAndCollectRecordsByFileName", ParDo.of(new ParseAndArchiveRowTransform()))

                        // Setting non-global windowing function to support Combine/GroupByKey
                        .apply(Window.into(FixedWindows.of(Duration.standardSeconds(10))));

        WriteResult writeResult =
                dataToBeProcessed
                        // 3.1.1) Process each line in accordance with the custom logic and construct a BigQuery TableRow per line
                        .apply("Transform Each Line", ParDo.of(new CsvToJsonTransform(options.getDelimiter())))

                        // 3.1.2) Persist the records into BigQuery table
                        .apply(
                                "InsertIntoBigQuery",
                                BigQueryIO.writeTableRows()
                                        .withJsonSchema(getSchemaFromGCS(options.getJSONPath()))
                                        .to(options.getOutputTable())
                                        .withExtendedErrorInfo()
                                        .withoutValidation()
                                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                        .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                                        .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                                        .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory()));

        // 3.1.3) Extract rows that failed to insert into BigQuery along with the error
        writeResult
                .getFailedInsertsWithErr()
                .apply("Aggregate Failed Rows", MapElements.into(TypeDescriptors.strings())
                        .via(CsvToBigQueryStreaming::wrapBigQueryInsertError))
                .apply(Window.into(FixedWindows.of(Duration.standardSeconds(10))))
                // 3.1.4) Persist the failed rows along with error message into an error file in GCS
                .apply("Write Errors To Error File",
                        TextIO.write()
                                .to(getErrorFile(options.getErrorFolder(), options.getJobName()))
                                .withWindowedWrites()
                                .withNumShards(5));

        return pipeline.run();
    }

    private static String getErrorFile(ValueProvider<String> errorFolder, String jobName) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        return errorFolder.get() + jobName + format.format(new Date()) + ".txt";
    }

    /**
     * Return a concatenated string of row and the error message in the format <code>[row],[errorMessage]</code>
     *
     * @param insertError
     * @return
     */
    private static String wrapBigQueryInsertError(BigQueryInsertError insertError) {
        String errorMessagePayload = "";
        try {
            String rowPayload = JSON_FACTORY.toString(insertError.getRow());
            String errorMessage = JSON_FACTORY.toString(insertError.getError());
            errorMessagePayload = "[" + rowPayload + "], [" + errorMessage + "]";
        } catch (IOException e) {
            LOG.error("Error occurred while parsing the error row or the error message into JSON", e);
        }

        return errorMessagePayload;
    }

    /**
     * Method to read a BigQuery schema file from GCS and return the file contents as a string.
     *
     * @param gcsPath Path string for the schema file in GCS.
     * @return File contents as a string.
     */
    private static ValueProvider<String> getSchemaFromGCS(ValueProvider<String> gcsPath) {
        return ValueProvider.NestedValueProvider.of(
                gcsPath,
                new SimpleFunction<String, String>() {
                    @Override
                    public String apply(String input) {
                        ResourceId sourceResourceId = FileSystems.matchNewResource(input, false);

                        String schema;
                        try (ReadableByteChannel rbc = FileSystems.open(sourceResourceId)) {
                            try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                                try (WritableByteChannel wbc = Channels.newChannel(baos)) {
                                    ByteStreams.copy(rbc, wbc);
                                    schema = baos.toString(Charsets.UTF_8.name());
                                    LOG.info("Extracted schema: " + schema);
                                }
                            }
                        } catch (IOException e) {
                            LOG.error("Error extracting schema: " + e.getMessage());
                            throw new RuntimeException(e);
                        }
                        return schema;
                    }
                });
    }
}
