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

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;

/**
 * This class converts the input keys and values to their String forms by
 * calling toString() method. This class to SequenceFileAsTextInputFormat
 * class is as LineRecordReader class to TextInputFormat class.
 */
public class SequenceFileAsRHTextRecordReader
  extends RecordReader<RHText, RHText> {
  
  private final SequenceFileRecordReader<WritableComparable<?>, Writable>
    sequenceFileRecordReader;

  private RHText key;
  private RHText value;

  public SequenceFileAsRHTextRecordReader()
    throws IOException {
    sequenceFileRecordReader =
      new SequenceFileRecordReader<WritableComparable<?>, Writable>();
  }

  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    sequenceFileRecordReader.initialize(split, context);
  }

  @Override
  public RHText getCurrentKey() 
      throws IOException, InterruptedException {
    return key;
  }
  
  @Override
  public RHText getCurrentValue() 
      throws IOException, InterruptedException {
    return value;
  }
  
  /** Read key/value pair in a line. */
  public synchronized boolean nextKeyValue() 
      throws IOException, InterruptedException {
    if (!sequenceFileRecordReader.nextKeyValue()) {
      return false;
    }
    if (key == null) {
      key = new RHText(); 
    }
    if (value == null) {
      value = new RHText(); 
    }
    key.setAndFinis(sequenceFileRecordReader.getCurrentKey().toString());
    value.setAndFinis(sequenceFileRecordReader.getCurrentValue().toString());
    return true;
  }
  
  public float getProgress() throws IOException,  InterruptedException {
    return sequenceFileRecordReader.getProgress();
  }
  
  public synchronized void close() throws IOException {
    sequenceFileRecordReader.close();
  }
}
