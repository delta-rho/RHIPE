package org.godhuli.rhipe;
import org.godhuli.rhipe.REXPProtos.REXP;
import org.godhuli.rhipe.REXPProtos.REXP.RClass;
import java.util.Vector;

public class RObjects {
    public RObjects(){
    }
    public static REXP makeStringVector(String[] s){
	REXP.Builder returnvalue   = REXP.newBuilder();
	returnvalue.setRclass(REXP.RClass.STRING);
	for(int i=0;i< s.length;i++){
	    REXPProtos.STRING.Builder content=REXPProtos.STRING.newBuilder();
	    content.setStrval(s[i]);
	    returnvalue.addStringValue(content.build());
	}
	return(returnvalue.build());
    }

    public static REXP makeStringVector(String s){
	return makeStringVector(new String[]{s});
    }
    public static REXP.Builder buildBooleanVector(boolean[] b){
	REXP.Builder cvalues = REXP.newBuilder();
	cvalues.setRclass(REXP.RClass.LOGICAL);
	for(int i=0;i<b.length;i++){
	    if(b[i])
		cvalues.addBooleanValue(org.godhuli.rhipe.REXPProtos.REXP.RBOOLEAN.T);
	    else
		cvalues.addBooleanValue(org.godhuli.rhipe.REXPProtos.REXP.RBOOLEAN.F);
	}
	return(cvalues);
    }
    public static REXP.Builder buildDoubleVector(double[] b){
	REXP.Builder cvalues = REXP.newBuilder();
	cvalues.setRclass(REXP.RClass.REAL);
	for(int i=0;i<b.length;i++){
	    cvalues.addRealValue(b[i]);
	}
	return(cvalues);
    }
    public static REXP.Builder buildIntVector(int[] b){
	REXP.Builder cvalues = REXP.newBuilder();
	cvalues.setRclass(REXP.RClass.INTEGER);
	for(int i=0;i<b.length;i++){
	    cvalues.addIntValue(b[i]);
	}
	return(cvalues);
    }

    public static REXP makeList(String[] names, Vector<REXP> rexp){
	REXP.Builder thevals   = REXP.newBuilder();
	thevals.setRclass(REXP.RClass.LIST);
	for(int i=0;i<rexp.size();i++){
	    thevals.addRexpValue(rexp.get(i));
	}
	thevals.addAttrName("names");
	thevals.addAttrValue(makeStringVector(names));
	return(thevals.build());
    }
    public static REXP makeList(Vector<String> names, Vector<REXP> rexp){
	return(makeList( names.toArray(new String[]{}), rexp));
    }
}