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
package org.saptarshiguha.rhipe.hadoop;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class uniWritable implements Writable {
    Object wble;
    int ty;
    public uniWritable(){  }
    public uniWritable(String a){
	wble=new Text(a);ty=1;
    }
    public uniWritable(int a){
	wble=new IntWritable(a);ty=2;
    }
    public uniWritable(byte[] a){
	wble=new BytesWritable(a);ty=3;
    }
    public uniWritable(String[] a){
	wble=new ArrayWritable(a);ty=4;
    }
    public uniWritable(MapWritable a){
	wble=a;ty=5;
    }
    public uniWritable(double a){
	wble=new DoubleWritable(a);ty=6;
    }
    public void readFields(DataInput in) throws IOException {
	IntWritable k= new IntWritable();
	k.readFields(in);
	int kk = k.get();
	if(kk==2){
	    wble = new IntWritable();
	}else if(kk==3){
	    wble = new BytesWritable();
	}else if(kk==4){
	    wble = new ArrayWritable(new String[]{});
	}else if(kk==5){
	    wble = new MapWritable();
	}else if(kk==1){
	    wble = new Text();
	}else if(kk==6){
	    wble = new DoubleWritable();
	}
	((Writable)wble).readFields(in);
    }
    public void write(DataOutput out) throws IOException{
	IntWritable k = new IntWritable(ty);
	k.write(out);
	((Writable)wble).write(out);
    }
    int getInt() {return( ((IntWritable)wble).get());}
    double getDouble() {return( ((DoubleWritable)wble).get());}
    String getString() { return(((Text)wble).toString());}
    String[] getStrings() { return(((ArrayWritable)wble).toStrings());}
    byte[] getBytes() { return(((BytesWritable)wble).getBytes());}
    MapWritable getMap(){ return((MapWritable)wble);}
}