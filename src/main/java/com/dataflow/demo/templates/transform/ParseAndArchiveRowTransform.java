package com.dataflow.demo.templates.transform;

import com.google.cloud.storage.*;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.Channels;

import static com.dataflow.demo.templates.util.FileUtil.getBucketName;
import static com.dataflow.demo.templates.util.FileUtil.getFileName;

public class ParseAndArchiveRowTransform extends DoFn<FileIO.ReadableFile, KV<String, String>> {
    private static final Logger LOG = LoggerFactory.getLogger(ParseAndArchiveRowTransform.class);

    private static void archiveFile(String fileNameUrl, CsvToBigQueryStreamingOptions options) {
        String archiveBucketName = getBucketName(options.getArchiveFolder().get());
        String sourceBucketName = getBucketName(fileNameUrl);
        String sourceFileName = getFileName(fileNameUrl);

        LOG.info("Attempting to copy {} from {} to {}", sourceFileName, sourceBucketName, archiveBucketName);
        Storage storage = StorageOptions.newBuilder().setProjectId(options.getProject()).build().getService();
        Blob blob = storage.get(BlobId.of(sourceBucketName, sourceFileName));
        CopyWriter copyWriter = blob.copyTo(archiveBucketName, sourceFileName);
        Blob copiedBlob = copyWriter.getResult();
        blob.delete();
        LOG.info("Moved object [{}] from bucket [{}] to bucket [{}]", sourceFileName, sourceBucketName, archiveBucketName);
    }

    @ProcessElement
    public void process(ProcessContext c) throws IOException {
        FileIO.ReadableFile readableFile = c.element();
        String fileNameUrl = readableFile.getMetadata().resourceId().toString();
        String line;
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(Channels.newInputStream(readableFile.open())))) {
            while ((line = bufferedReader.readLine()) != null) {
                c.output(KV.of(fileNameUrl, line));
            }
        }

        // Move file to archive directory
        CsvToBigQueryStreamingOptions options = c.getPipelineOptions().as(CsvToBigQueryStreamingOptions.class);
        archiveFile(fileNameUrl, options);
    }
}
