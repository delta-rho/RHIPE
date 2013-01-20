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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.google.common.base.Objects;

public class ValuePair {
    String path = null;
    RHBytesWritable key = null;
    protected static final Log LOG = LogFactory.getLog(PersonalServer.class
						       .getName());

    public ValuePair(){
    }
    public ValuePair(String s, RHBytesWritable r){
	path=s;
	key=r;
    }
    public void setKey(RHBytesWritable k){
	key = k;
    }
    public void setPath(String s){
	path = s;
    }
    RHBytesWritable getKey(){
	return key;
    }
    String getPath(){
	return path;
    }
    public int hashCode(){
	int hash;
    	// int hash = 1;
    	// hash = hash * 31 + key.hashCode();
    	// hash = hash * 31 + path.hashCode();
	hash = Objects.hashCode(key,path);
	return(hash);
    }
    public boolean equals(Object o){
	if (!(o instanceof ValuePair))
	    return false;
	ValuePair oo = (ValuePair)o;
	return path.equals(oo.getPath()) && key.equals(oo.getKey());
    }
    public String toString(){
	return "path:"+path+" key:"+key;
    }
}
