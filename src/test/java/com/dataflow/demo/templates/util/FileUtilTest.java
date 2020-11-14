package com.dataflow.demo.templates.util;

import org.junit.Test;

import static com.dataflow.demo.templates.util.FileUtil.getBucketName;
import static com.dataflow.demo.templates.util.FileUtil.getFileName;
import static org.junit.Assert.assertEquals;

public class FileUtilTest {

    @Test
    public void testFIleNAmeExtraction() {
        String fileName1 = "gs://bucket/folder1/folder2/file1.txt";
        assertEquals("file1.txt", getFileName(fileName1));

        String fileName2 = "gs://bucket/folder1/folder2/file1";
        assertEquals("file1", getFileName(fileName2));

        String fileName3 = "gs://bucket/folder1/folder2/*.csv";
        assertEquals("*.csv", getFileName(fileName3));
    }

    @Test
    public void testBucketNameExtraction() {
        String url1 = "gs://bucket/folder1/folder2";
        assertEquals("bucket", getBucketName(url1));

        String url2 = "gs://bucket/";
        assertEquals("bucket", getBucketName(url2));

        String url3 = "gs://bucket";
        assertEquals("bucket", getBucketName(url3));

        String url4 = "bucket";
        assertEquals("bucket", getBucketName(url4));
    }
}