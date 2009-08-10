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
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.BinaryComparable;
import org.rosuda.REngine.*;
import org.rosuda.REngine.Rserve.*;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;


public interface RXWritable {
    public REXP getREXP() throws IOException;
    public void set(REXP r) throws REXPMismatchException;
}
