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


import org.godhuli.rhipe.REXPProtos.REXP;
import org.godhuli.rhipe.REXPProtos.REXP.RClass;
import org.apache.hadoop.mapreduce.Partitioner;

public class RHPartitionerInteger extends Partitioner<RHBytesWritable,RHBytesWritable>{

    public int getPartition(RHBytesWritable key, RHBytesWritable value,
			    int numReduceTasks) {
	int hashcode = 0;
	// this is a crude and almost uses paritioning scheme.
	try{
	    REXP r = key.getParsed();
	    for(int i=RHMRHelper.PARTITION_START;i<=RHMRHelper.PARTITION_END;i++){
		hashcode = 10*hashcode + r.getIntValue(i);
	    }

	}catch(com.google.protobuf.InvalidProtocolBufferException e){
	    System.err.println(e);
	}
	int a= (hashcode & Integer.MAX_VALUE) % numReduceTasks;
	return(a);
    }
  // protected int hashCode(byte[] b, int currentHash) {
  //   for (int i = 0; i < b.length; i++) {
  //     currentHash = 31*currentHash + b[i];
  //   }
  //   return currentHash;
  // }

}