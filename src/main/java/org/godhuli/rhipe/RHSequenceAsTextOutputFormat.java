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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * An {@link OutputFormat} that writes {@link SequenceFile}s.
 */
public class RHSequenceAsTextOutputFormat extends FileOutputFormat<RHBytesWritable, RHBytesWritable> {
    class ElementWriter extends RecordWriter<RHBytesWritable, RHBytesWritable> {
        final Text a;
        final Text b;
        final SequenceFile.Writer o;

        public ElementWriter(final SequenceFile.Writer out, final String sq) {
            a = new Text();
            b = new Text();
            o = out;
            REXPHelper.setStringQuote(sq);
        }

        public void write(final RHBytesWritable key, final RHBytesWritable value) throws IOException {
            a.set(key.toString());
            b.set(value.toString());
            o.append(a, b);
        }

        public void close(final TaskAttemptContext context) throws IOException {
            o.close();
        }
    }

    public RecordWriter<RHBytesWritable, RHBytesWritable> getRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException {
        final Configuration conf = context.getConfiguration();
        CompressionCodec codec = null;
        CompressionType compressionType = CompressionType.NONE;
        String squote = conf.get("rhipe_string_quote");
        if (squote == null) {
            squote = "";
        }
        if (getCompressOutput(context)) {
            compressionType = getOutputCompressionType(context);
            final Class<?> codecClass = getOutputCompressorClass(context, DefaultCodec.class);
            codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
        }
        final Path file = getDefaultWorkFile(context, "");
        final FileSystem fs = file.getFileSystem(conf);
//        final SequenceFile.Writer out = SequenceFile.createWriter(fs, conf, file, org.apache.hadoop.io.Text.class, org.apache.hadoop.io.Text.class, compressionType, codec, context);
        final SequenceFile.Writer out = SequenceFile.createWriter(conf,SequenceFile.Writer.file(file),
                                                                     SequenceFile.Writer.keyClass(org.apache.hadoop.io.Text.class),
                                                                     SequenceFile.Writer.valueClass(org.apache.hadoop.io.Text.class),
                                                                     SequenceFile.Writer.compression(compressionType, codec),
                                                                     SequenceFile.Writer.progressable(context));
        return new ElementWriter(out, squote);
    }

    public static CompressionType getOutputCompressionType(final JobContext job) {
        final String val = job.getConfiguration().get("mapred.output.compression.type", CompressionType.BLOCK.toString());
        return CompressionType.valueOf(val);
    }

    public static void setOutputCompressionType(final Job job, final CompressionType style) {
        setCompressOutput(job, true);
        job.getConfiguration().set("mapred.output.compression.type", style.toString());
    }
}

