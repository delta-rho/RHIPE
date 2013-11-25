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
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;


public class RXTextOutputFormat extends FileOutputFormat<RHBytesWritable, RHBytesWritable> {

    protected static class RXTextRecordWriter extends RecordWriter<RHBytesWritable, RHBytesWritable> {
        private byte[] newLine = "\r\n".getBytes();
        private static byte[] keyvaluesep = " ".getBytes();
        private static final String utf8 = "UTF-8";
        private final boolean useKey;
        protected final DataOutputStream out;


        public RXTextRecordWriter(final DataOutputStream out, final String keyValueSeparator, final String fieldSep, final String squote, final boolean useKey, final String newline) {
            this.out = out;
            this.useKey = useKey;
            this.newLine = newline.getBytes();
            try {
                keyvaluesep = keyValueSeparator.getBytes(utf8);
                REXPHelper.setFieldSep(fieldSep);
                REXPHelper.setStringQuote(squote);
            }
            catch (UnsupportedEncodingException uee) {
                throw new IllegalArgumentException("can't find " + utf8 + " encoding");
            }

        }

        public synchronized void write(final RHBytesWritable key, final RHBytesWritable value) throws IOException {
            if (useKey) {
                out.write(key.toString().getBytes(utf8));
                out.write(keyvaluesep);
            }
            out.write(value.toString().getBytes(utf8));
            out.write(newLine, 0, newLine.length);

            // System.out.println("Key="+key.toString());
            // System.out.println("Value="+value.toString());
        }


        public synchronized void close(final TaskAttemptContext context) throws IOException {
            out.close();
        }


    }


    public RecordWriter<RHBytesWritable, RHBytesWritable> getRecordWriter(final TaskAttemptContext job) throws IOException, InterruptedException {
        final Configuration conf = job.getConfiguration();
        final boolean isCompressed = getCompressOutput(job);
        final String keyValueSeparator = conf.get("mapred.textoutputformat.separator", "\t");
        final String fieldSeparator = conf.get("mapred.field.separator", " ");
        final boolean usekey = conf.get("mapred.textoutputformat.usekey").equals("TRUE");
        final String newline = conf.get("rhipe.eol.sequence");
        String squote = conf.get("rhipe_string_quote");
        if (squote == null) {
            squote = "";
        }
        CompressionCodec codec = null;
        String extension = "";
        if (isCompressed) {
            final Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
            codec = ReflectionUtils.newInstance(codecClass, conf);
            extension = codec.getDefaultExtension();
        }
        final Path file = getDefaultWorkFile(job, extension);
        final FileSystem fs = file.getFileSystem(conf);
        if (!isCompressed) {
            final FSDataOutputStream fileOut = fs.create(file, false);
            return new RXTextRecordWriter(fileOut, keyValueSeparator, fieldSeparator, squote, usekey, newline);
        }
        else {
            final FSDataOutputStream fileOut = fs.create(file, false);
            return new RXTextRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)), keyValueSeparator, fieldSeparator, squote, usekey, newline);
        }

    }
}
