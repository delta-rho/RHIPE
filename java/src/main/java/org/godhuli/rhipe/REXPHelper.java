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



public class REXPHelper  {
    public static String fsep = " ";
    public static String squote = "\"";

    public static void setFieldSep(String s){ fsep=s;}
    public static void setStringQuote(String s){ squote=s;}

    public static String debugString(byte[] b){
	REXP r = null;
	try{
	    r=REXP.parseFrom(b);
	}catch( com.google.protobuf.InvalidProtocolBufferException e){
	    return(null);
	}
	return(r.toString());
    }

    public static String toString(byte[] b,int off, int len){
	REXP r = null;
	try{
		
	    r=REXP.newBuilder().mergeFrom(b,off,len).build();
	}catch( com.google.protobuf.InvalidProtocolBufferException e){
	    return(null);
	}
	String s = toString_(r);
	return(s);
    }

    public static String toString_(REXP r){
	REXP.RClass clz = r.getRclass();
	switch(clz){
	case STRING:
	    {
		StringBuilder sb = new StringBuilder();
		int length = r.getStringValueCount();
		for(int i=0;i<length-1;i++){
		    org.godhuli.rhipe.REXPProtos.STRING si = r.getStringValue(i);
		    if( si.getIsNA() )
			sb.append("NA");
		    else{
			sb.append(squote);sb.append(si.getStrval());
			sb.append(squote);
		    }
		    sb.append(fsep);
		}
		org.godhuli.rhipe.REXPProtos.STRING si = r.getStringValue(length-1);
		if( si.getIsNA() )
		    sb.append("NA");
		else{
		    sb.append(squote);sb.append(si.getStrval());
		    sb.append(squote);
		}
		return(sb.toString());
	    }
	case REAL:
	    {
		StringBuilder sb = new StringBuilder();
		int length = r.getRealValueCount();
		for(int i=0;i<length-1;i++){
		    sb.append(r.getRealValue(i));sb.append(fsep);
		}
		sb.append(r.getRealValue(length-1));
		return(sb.toString());
	    }
	case INTEGER:
	    {
		StringBuilder sb = new StringBuilder();
		int length = r.getIntValueCount();
		for(int i=0;i<length-1;i++){
		    sb.append(r.getIntValue(i));sb.append(fsep);
		}
		sb.append(r.getIntValue(length-1));
		return(sb.toString());
	    }
	case LOGICAL:
	    {
		StringBuilder sb = new StringBuilder();
		int length = r.getBooleanValueCount();
		for(int i=0;i<length-1;i++){
		    REXP.RBOOLEAN rb = r.getBooleanValue(i);
		    if(rb == REXP.RBOOLEAN.T) sb.append("TRUE");
		    else if(rb== REXP.RBOOLEAN.F) sb.append("FALSE");
		    else sb.append("NA");
		    sb.append(fsep);
		}
		REXP.RBOOLEAN rb = r.getBooleanValue(length-1);
		if(rb == REXP.RBOOLEAN.T) sb.append("TRUE");
		else if(rb== REXP.RBOOLEAN.F) sb.append("FALSE");
		else sb.append("NA");
		return(sb.toString());	
	    }
	case RAW:
	    {
		StringBuilder sb = new StringBuilder();
		byte[] rawvals = r.getRawValue().toByteArray();
		int length = rawvals.length;
		for(int i=0;i<length-1;i++){
		    sb.append("0x");sb.append(rawvals[i]);sb.append(fsep);
		}
		sb.append("0x");sb.append(rawvals[length-1]);
		return(sb.toString());
	    }
	case COMPLEX:
	    {
		StringBuilder sb = new StringBuilder();
		int length = r.getComplexValueCount();
		for(int i=0;i<length-1;i++){
		    org.godhuli.rhipe.REXPProtos.CMPLX ci = r.getComplexValue(i);
		    sb.append(ci.getReal());sb.append(ci.getImag());sb.append("i");
		    sb.append(fsep);
		}
		org.godhuli.rhipe.REXPProtos.CMPLX ci = r.getComplexValue(length-1);
		sb.append(ci.getReal());sb.append("+");sb.append(ci.getImag());sb.append("i");
		sb.append(fsep);
		return(sb.toString());
	    }
	case LIST:
	    {
		StringBuilder sb = new StringBuilder();
		int length = r.getRexpValueCount();
		for(int i=0;i < length-1;i++){
		    sb.append( toString_( r.getRexpValue(i) ));
		    sb.append(fsep);
		}
		sb.append( toString_( r.getRexpValue(length-1)));
		return(sb.toString());
	    }
	case NULLTYPE:
	    {
		return("NULL");
	    }
	default:
	    return("\"Not Recongizable R Object\"");
	}
    }
}
