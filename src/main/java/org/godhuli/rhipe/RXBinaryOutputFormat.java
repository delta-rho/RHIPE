/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.godhuli.rhipe;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class RXBinaryOutputFormat extends FileOutputFormat<RHBytesWritable, RHBytesWritable> {

    public RecordWriter<RHBytesWritable, RHBytesWritable> getRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException {
        final Configuration conf = context.getConfiguration();
        final Path file = getDefaultWorkFile(context, "");
        final FileSystem fs = file.getFileSystem(conf);
        final FSDataOutputStream out = fs.create(file, false);
        return new RecordWriter<RHBytesWritable, RHBytesWritable>() {
            int counter = 0;

            public void write(final RHBytesWritable key, final RHBytesWritable value) throws IOException {
                key.writeAsInt(out);
                value.writeAsInt(out);
//                out.sync();
                out.hflush();
            }

            public void close(final TaskAttemptContext context) throws IOException {
                out.close();
            }
        };
    }
}

