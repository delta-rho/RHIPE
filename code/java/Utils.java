/*
 * RHIPE - software that integrates Hadoop mapreduce with R
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * Saptarshi Guha sguha@purdue.edu
 */
package org.saptarshiguha.rhipe.utils;
import java.lang.StringBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import java.util.Random;
import java.util.Vector;
import org.rosuda.REngine.*;
import org.rosuda.REngine.Rserve.*;
import java.io.*;
import java.net.*;
import org.saptarshiguha.rhipe.hadoop.RXWritableRAW;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Iterator;


public class Utils {
    public Utils(){}
    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();
    public static enum  ERRTYPE { CLOSE,CONFIG,MAP,CMB,RED,LAPPLY;}
    public static byte[] forbytes(Object k){
	byte[] x = (byte[])k;
	return(x);
    }
    public static void assignConfVars(Configuration c, RConnection r) throws RserveException{
	Iterator<Map.Entry<String,String>> it = c.iterator();
	RList rl = new RList();
	int i=0;
	while(it.hasNext()){
	    Map.Entry<String,String> v = it.next();
	    rl.add(  new REXPString(v.getValue()));
	    rl.setKeyAt(i,v.getKey());i++;
	}
	REXPGenericVector rv = new REXPGenericVector(rl);
	r.assign("mapred.opts",rv);
    }

    public static String prettyPrintHex(byte[] data){
	return(prettyPrintHex(data,0,data.length));
    }
    public static void showError(RConnection re,RList r,String title,ERRTYPE et) throws RserveException,IOException,REXPMismatchException {
	if(r.containsKey("e")) {
	    String intro=title+"\n"+Utils.build(r.at("e").asStrings(),"\n");
	    String body = null;
	    if(et==ERRTYPE.MAP){
		String ke = build(re.eval("capture.output(head(mapdata$key))").asStrings(),"\n");
		String ve = build(re.eval("capture.output(head(mapdata$value))").asStrings(),"\n");
		body="Offending head(KEY):\n"+ke+"\nOffending head(VALUE):\n"+ve;
	    }else if(et==ERRTYPE.RED || et == ERRTYPE.CMB){
		String ke = build(re.eval("capture.output(head(red.key))").asStrings(),"\n");
		String ve = build(re.eval("capture.output(head(red.value))").asStrings(),"\n");
		body="Offending head(KEY):\n"+ke+"\nOffending head(VALUE):\n"+ve;
	    }else if(et==ERRTYPE.LAPPLY){
		String ke = build(re.eval("capture.output(head(mapdata))").asStrings(),"\n");
		body="Offending head(INDEX):\n"+ke;
	    }



	    throw new IOException(intro+body);
	}
    }
    public static void showStdout(RList r,String title)throws REXPMismatchException {
	if(r.containsKey("s")){
	    String[] v = r.at("s").asStrings();
	    if(v.length>0){
		String fout = Utils.build(v,"\n");
		System.out.println(title);
		System.out.println(fout);
	    }
	}
    }
    public static String build(String[] v,String jw){
	StringBuilder sb = new StringBuilder();
	for(int h=0;h<v.length;h++){
	    sb.append(v[h]);
	    sb.append(jw);
	}
	return sb.toString();
    }
    public static String build(String[] v){
	return Utils.build(v,"");
    }
    public static void loadFuncs(RConnection re) throws Exception{
	StringBuffer b=new StringBuffer();
	try{
	    URL url = Utils.class.getResource("/rstuff.txt");
	    BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));
	    String str;
	    while ((str = in.readLine()) != null) {
		b.append(str);b.append("\n");
		// str is one line of text; readLine() strips the newline character(s)
	    }
	    in.close();
	} catch (MalformedURLException e) { throw new Exception(e);
	} catch (IOException e) { throw new Exception(e);
	}
	re.voidEval(b.toString());
    }


    public static int sdd(){
// 	byte[] sd = SecureRandom.getSeed(8);
// 	long sdi = (((long)(sd[0] & 0xff) << 56) |
// 		    ((long)(sd[1] & 0xff) << 48) |
// 		    ((long)(sd[2] & 0xff) << 40) |
// 		    ((long)(sd[3] & 0xff) << 32) |
// 		    ((long)(sd[4] & 0xff) << 24) |
// 		    ((long)(sd[5] & 0xff) << 16) |
// 		    ((long)(sd[6] & 0xff) <<  8) |
// 		    ((long)(sd[7] & 0xff)));
	Random r = new Random();
	int d=  r.nextInt( Integer.MAX_VALUE );
	return(d);
    }
    public static void deleteFonDFS(String file,int loc){
	try{
	    Configuration defaults = new Configuration();
	    FileSystem.get(defaults).delete(new Path(file), true);
	}catch(Exception e){
	    e.printStackTrace();
	}
    }
    public static String prettyPrintHex(byte[] data,int start,int max) {
	int i = 0, j = 0;   // loop counters
	int line_addr = 0;  // memmory address printed on the left
	String line_to_print = "";
	if(max>data.length) max=data.length;
	if(start <0 ) start=0;
	if(start > data.length) start=data.length;
	if (max<start){
	    int t=start;
	    start=max;max=start;
	}
	if (data.length == 0) {
	    return (new String());
	}
	StringBuilder _sbbuffer = new StringBuilder();
	//Loop through every input byte
	String _hexline = "";
	String _asciiline = "";
	for (i = start, line_addr = 0; i < max; i++, line_addr++) {
	    //Print the line numbers at the beginning of the line
	    if ((i % 16) == 0) {
		if (i != 0) {
		    _sbbuffer.append(_hexline);
		    _sbbuffer.append("\t...\t");
		    _sbbuffer.append(_asciiline + "\n");
		}
		_asciiline = "";
		_hexline = String.format("%#06x ", line_addr);
	    }
	    _hexline = _hexline.concat(String.format("%#04x ", data[i]));
	    if (data[i] > 31 && data[i] < 127) {
		_asciiline = _asciiline.concat(String.valueOf((char) data[i]));
	    } else {
		_asciiline = _asciiline.concat(".");
	    }
	}
	// Handle the ascii for the final line, which may not be completely filled.
	if (i % 16 > 0) {
	    for (j = 0; j < 16 - (i % 16); j++) {
		_hexline = _hexline.concat("     ");
	    }
	    _sbbuffer.append(_hexline);
	    _sbbuffer.append("\t...\t");
	    _sbbuffer.append(_asciiline);
	}
	return(_sbbuffer.toString());
    }
    
    public static void writeAndCloseSeq(FileSystem fs,Configuration cfg,String name,Vector<byte[]> kve, Vector<byte[]> vve, 
					RXWritableRAW kv, RXWritableRAW vv) throws IOException{

	SequenceFile.Writer wr =  SequenceFile.createWriter(fs,cfg, new Path(name), RXWritableRAW.class, RXWritableRAW.class,SequenceFile.CompressionType.NONE);
	for(int i=0;i< kve.size(); i++){
	    kv.set(kve.get(i)); vv.set(vve.get(i));
	    wr.append(kv,vv);
	}
	wr.close();
    }
    public static Vector<byte[]> givemeVec(){
	Vector<byte[]> v = new Vector<byte[]>();
	return(v);
    }
    public static void addKVToAVec(Vector<byte[]> k,Vector<byte[]> v,byte[] kr, byte[] vr){
	k.add(kr);
	v.add(vr);

    }

}
