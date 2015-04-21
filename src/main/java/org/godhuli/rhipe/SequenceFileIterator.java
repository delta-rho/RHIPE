package org.godhuli.rhipe;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.godhuli.rhipe.REXPProtos.REXP;

import java.io.IOException;

public class SequenceFileIterator {
    protected static final Log LOG = LogFactory.getLog(SequenceFileIterator.class.getName());

    String[] files;
    int current;
    int chunk;
    RHBytesWritable k, v;
    Text kt, vt;
    SequenceFile.Reader sqr;
    boolean notcomplete;
    FileSystem fs;
    Configuration cfg;
    int mnum;
    int numreadtill = 0;
    boolean textual;

    public SequenceFileIterator() {
    }

    public void init(final String filenames, final int chunksize, final int maxn, final PersonalServer s) throws IOException {
        init(new String[]{filenames}, chunksize, maxn, s);
    }

    public void init(final String[] filenames, final int chunksize, final int maxn, final PersonalServer s) throws IOException {
        files = filenames;
        chunk = chunksize;
        current = 0;
        cfg = s.getConf();
	if(filenames.length == 0) throw new IOException("RHIPE: files names has zero length");
	fs = (new Path( filenames[0])).getFileSystem(cfg);
        notcomplete = true;
        mnum = maxn;
        sqr = new SequenceFile.Reader(cfg, SequenceFile.Reader.file(new Path(files[current])));
        k = new RHBytesWritable();
        v = new RHBytesWritable();
        kt = new Text();
        vt = new Text();

    }

    public static SequenceFile.Reader openAFile(final FileSystem fs, final String p, final Configuration c) throws IOException {
//        return new SequenceFile.Reader(fs, new Path(p), c);
        return new SequenceFile.Reader(c, SequenceFile.Reader.file(new Path(p)));

    }

    public void setTextual(final boolean a) {
        textual = a;
    }

    public boolean hasMoreElements() {
        return notcomplete && (mnum < 0 || numreadtill < mnum);
    }

    public byte[] nextElement() throws Exception {
        final REXP.Builder thevals = REXP.newBuilder();
        thevals.setRclass(REXP.RClass.LIST);
        boolean gotone = false;
        for (int i = 0; i < chunk && (mnum < 0 || numreadtill < mnum); i++) {
            if (textual) {
                gotone = sqr.next(kt, vt);
            }
            else {
                gotone = sqr.next(k, v);
            }

            if (gotone) {
                numreadtill++;
                final REXP.Builder a = REXP.newBuilder();
                a.setRclass(REXP.RClass.LIST);
                if (textual) {
                    final byte[] a0 = RObjects.makeStringVector(Text.decode(kt.getBytes(), 0, kt.getLength())).toByteArray();
                    final byte[] a1 = RObjects.makeStringVector(Text.decode(vt.getBytes(), 0, vt.getLength())).toByteArray();
                    a.addRexpValue(RObjects.buildRawVector(a0));
                    a.addRexpValue(RObjects.buildRawVector(a1));
                }
                else {
                    a.addRexpValue(RObjects.buildRawVector(k.getBytes(), 0, k.getLength()));
                    a.addRexpValue(RObjects.buildRawVector(v.getBytes(), 0, v.getLength()));
                }
                a.build();
                thevals.addRexpValue(a);
            }
            else {
                sqr.close();
                current++;
                if (current == files.length) {
                    notcomplete = false;
                    break;
                }
                sqr = new SequenceFile.Reader(cfg, SequenceFile.Reader.file(new Path(files[current])));
//                sqr = new SequenceFile.Reader(fs, new Path(files[current]), cfg);
                // LOG.info(sqr);
            }
        }
        return thevals.build().toByteArray();
    }

    public byte[] nextChunk() throws Exception {
        final REXP.Builder thevals = REXP.newBuilder();
        thevals.setRclass(REXP.RClass.LIST);
        boolean gotone = false;
        int bread = 0;
        int TKL = 0;
        while (true) {
            if (textual) {
                gotone = sqr.next(kt, vt);
                TKL = kt.getLength() + vt.getLength();
            }
            else {
                gotone = sqr.next(k, v);
                TKL = k.getLength() + v.getLength();
            }
            if (gotone) {
                numreadtill++;
                bread += TKL;
                final REXP.Builder a = REXP.newBuilder();
                a.setRclass(REXP.RClass.LIST);
                if (textual) {
                    final byte[] a0 = RObjects.makeStringVector(Text.decode(kt.getBytes(), 0, kt.getLength())).toByteArray();
                    final byte[] a1 = RObjects.makeStringVector(Text.decode(vt.getBytes(), 0, vt.getLength())).toByteArray();
                    a.addRexpValue(RObjects.buildRawVector(a0));
                    a.addRexpValue(RObjects.buildRawVector(a1));
                }
                else {
                    a.addRexpValue(RObjects.buildRawVector(k.getBytes(), 0, k.getLength()));
                    a.addRexpValue(RObjects.buildRawVector(v.getBytes(), 0, v.getLength()));
                }
                a.build();
                thevals.addRexpValue(a);
            }
            else {
                sqr.close();
                current++;
                if (current == files.length) {
                    notcomplete = false;
                    break;
                }
                // LOG.info("switching to next file: "+files[current]);
                sqr = new SequenceFile.Reader(cfg, SequenceFile.Reader.file(new Path(files[current])));
//                sqr = new SequenceFile.Reader(fs, new Path(files[current]), cfg);
            }
            if (bread > chunk || numreadtill == mnum) {
                break;
            }
        }
        return thevals.build().toByteArray();
    }


}
