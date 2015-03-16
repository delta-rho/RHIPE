// /**
//  * Licensed to the Apache Software Foundation (ASF) under one
//  * or more contributor license agreements.  See the NOTICE file
//  * distributed with this work for additional information
//  * regarding copyright ownership.  The ASF licenses this file
//  * to you under the Apache License, Version 2.0 (the
//  * "License"); you may not use this file except in compliance
//  * with the License.  You may obtain a copy of the License at
//  *
//  *     http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS,
//  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  * See the License for the specific language governing permissions and
//  * limitations under the License.
//  */
package org.godhuli.rhipe;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.godhuli.rhipe.REXPProtos.REXP;

import java.io.IOException;


public class RHWriter {
    protected static final Log LOG = LogFactory.getLog(RHWriter.class.getName());

    private SequenceFile.Writer sqw;
    final Configuration c;
    final FileSystem f;
    final String dest;
    int currentfile;
    final int numperfile;
    int numwritten = 0;
    final RHBytesWritable anull = new RHBytesWritable((new RHNull()).getRawBytes());

    public RHWriter(final String output, final int numper, final PersonalServer s) throws Exception {
        dest = output;
        currentfile = 0;
        numperfile = numper;
        c = s.getConf();
        f = (new Path(output)).getFileSystem(s.getConf()); // default FS based on conf, is hdfs
        makeNewFile();
    }

    public void write(final byte[] d, final boolean b) throws Exception {
        try {
            final REXP elements = REXP.newBuilder().mergeFrom(d, 0, d.length).build();
            final RHBytesWritable x = new RHBytesWritable();
            final RHBytesWritable y = new RHBytesWritable();
            for (int i = 0; i < elements.getRexpValueCount(); i++) {
                final REXP ie = elements.getRexpValue(i);
                // LOG.info("Numwritten="+numwritten);
                if (numwritten >= numperfile) {
                    close();
                    makeNewFile();
                }
                if (b) {
                    x.set(ie.toByteArray());
                    sqw.append(anull, x);
                }
                else {
                    final REXP ie2 = ie.getRexpValue(0);
                    x.set(ie2.getRexpValue(0).toByteArray());
                    y.set(ie2.getRexpValue(1).toByteArray());
                    sqw.append(x, y);
                }
                numwritten++;
            }
        }
        catch (IOException e) {
            sqw.close();
        }
    }

    public void makeNewFile() throws IOException {
        currentfile++;
        // LOG.info("New File for "+currentfile);
//        sqw = new SequenceFile.Writer(f, c, new Path(dest + "/part_" + currentfile), RHBytesWritable.class, RHBytesWritable.class);
        sqw = SequenceFile.createWriter(c,SequenceFile.Writer.file(new Path(dest + "/part_" + currentfile)),
                                           SequenceFile.Writer.keyClass(RHBytesWritable.class),
                                           SequenceFile.Writer.valueClass(RHBytesWritable.class));
        numwritten = 0;
    }

    public void close() throws IOException {
        // LOG.info("Closed file corresponding to "+currentfile);
        if (sqw != null) {
            sqw.sync();
            sqw.close();
        }
    }

}

