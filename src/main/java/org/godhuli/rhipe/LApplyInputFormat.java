/**
 * Copyright 2009 Saptarshi Guha
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.godhuli.rhipe;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class LApplyInputFormat extends InputFormat<RHNumeric, RHNumeric> {
    public LApplyInputFormat() {
    }

    public List<InputSplit> getSplits(final JobContext job) throws IOException {
        final int n = job.getConfiguration().getInt("rhipe_lapply_lengthofinput", 0);
        final List<InputSplit> splits = new ArrayList<InputSplit>();
        int numSplits = job.getConfiguration().getInt("mapred.map.tasks", 0);
        final int chunkSize;

        if (n <= numSplits) {
            numSplits = n;
            chunkSize = 1;
        }
        else {
            chunkSize = n / numSplits;
        }

        for (int i = 0; i < numSplits; i++) {
            final LApplyInputSplit split;
            if ((i + 1) == numSplits) {
                split = new LApplyInputSplit(i * chunkSize, n);
            }
            else {
                split = new LApplyInputSplit(i * chunkSize, (i * chunkSize) + chunkSize);
            }
            splits.add(split);
        }
        return splits;
    }

    public RecordReader<RHNumeric, RHNumeric> createRecordReader(final InputSplit split, final TaskAttemptContext tac) throws IOException, InterruptedException {
        return new LApplyReader((LApplyInputSplit) split, tac);
    }

    protected static class LApplyReader extends RecordReader<RHNumeric, RHNumeric> {
        private final LApplyInputSplit split;
        private long leftover;
        private long pos = 0;
        private RHNumeric key = null;
        private RHNumeric value = null;

        protected LApplyReader(final LApplyInputSplit split, final TaskAttemptContext tac) throws IOException {
            this.split = split;
            this.leftover = split.getLength();
        }

        public void initialize(final InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {
        }

        public void close() throws IOException {
        }

        public RHNumeric getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        public RHNumeric getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        public long getPos() {
            return this.pos;
        }

        public float getProgress() throws IOException {
            return ((float) this.pos) / (float) this.split.getLength();
        }

        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (key == null) {
                key = new RHNumeric();
            }
            if (value == null) {
                value = new RHNumeric();
            }
            if (leftover == 0) {
                return false;
            }
            final long wi = pos + split.getStart();
            key.setAndFinis(wi + 1);
            value.setAndFinis(wi + 1);
            pos++;
            leftover--;
            return true;
        }
    }

    protected static class LApplyInputSplit extends InputSplit implements Writable {
        private long end = 0;
        private long start = 0;

        public LApplyInputSplit() {
            super();
        }

        /**
         * Convenience Constructor
         *
         * @param start the index of the first row to select
         * @param end   the index of the last row to select (non-inclusive)
         */
        public LApplyInputSplit(final long start, final long end) {
            this.start = start;
            this.end = end - 1;
        }

        /**
         * @return The index of the first row to select
         */
        public long getStart() {
            return start;
        }

        /**
         * @return The index of the last row to select
         */
        public long getEnd() {
            return end;
        }

        /**
         * @return The total row count in this split
         */
        public long getLength() throws IOException {
            return end - start + 1;
        }

        public String[] getLocations() {
            return new String[]{};
        }

        public void readFields(final DataInput input) throws IOException {
            start = input.readLong();
            end = input.readLong();
        }

        public void write(final DataOutput output) throws IOException {
            output.writeLong(start);
            output.writeLong(end);
        }

    }


}

