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

import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class CsvToBigQueryStreamingTest
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue()
    {


        String a = "gs://aaa/bb/ccc/*.txt";
        String b = "gs://aaa/bb/ccc/dddd.txt";

        String c = "gs://aaa/bb/ccc/dddd.txt";

        String removeGs = (a.substring(a.lastIndexOf("//") + 2, a.length()));
        System.out.println(removeGs);
        System.out.println(removeGs.substring(0, removeGs.indexOf("/")));


    }
}
