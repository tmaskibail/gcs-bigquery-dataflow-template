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

package com.dataflow.demo.templates.transform;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.io.Serializable;

public class CsvToJsonTransform extends DoFn<KV<String, String>, TableRow> implements Serializable {

    private final ValueProvider<String> delimiter;

    public CsvToJsonTransform(ValueProvider<String> delimiter) {
        this.delimiter = delimiter;
    }

    /**
     * This is a demo transformation for files with handful of columns.
     *
     * @param line - Map of key (fileName), value(line)
     * @param out  - TableRow representation of the line
     */
    @ProcessElement
    public void processElement(@Element KV<String, String> line, OutputReceiver<TableRow> out) {
        String[] columns = line.getValue().split(delimiter.get());

        // Skip header row
        if ("Series_reference".equalsIgnoreCase(columns[0])) {
            return;
        }

        TableRow row =
                new TableRow()
                        // To learn more about BigQuery data types and sample code:
                        // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
                        // https://beam.apache.org/documentation/io/built-in/google-bigquery/
                        .set("seriesReference", columns[0])
                        .set("period", columns[1])
                        .set("dataValue", columns[2])
                        // Adding a new column that us based on column[2]
                        .set("newDataValue", (Float.parseFloat(columns[2]) * 100))
                        // Skip column[3] and move to column[4]
                        .set("status", columns[4])
                        .set("unit", columns[5]);
        out.output(row);
    }
}
