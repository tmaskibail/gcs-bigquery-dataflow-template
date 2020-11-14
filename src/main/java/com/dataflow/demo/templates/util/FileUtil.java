package com.dataflow.demo.templates.util;

public class FileUtil {
    private FileUtil() {
        throw new IllegalStateException("Utility class");
    }

    public static String getFileName(String fileNameUrl) {
        return fileNameUrl.substring(fileNameUrl.lastIndexOf("/") + 1);
    }

    public static String getBucketName(String fileNameUrl) {
        String bucketName;
        if (fileNameUrl.contains("//")) {
            bucketName = fileNameUrl.substring(fileNameUrl.lastIndexOf("//") + 2);
        } else {
            bucketName = fileNameUrl;
        }

        if (bucketName.contains("/")) {
            return bucketName.substring(0, bucketName.indexOf("/"));
        }
        return bucketName;
    }
}
