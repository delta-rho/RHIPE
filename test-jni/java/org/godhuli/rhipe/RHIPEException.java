package org.godhuli.rhipe;

public class RHIPEException extends Exception {
    private String x;
    public RHIPEException(String e){
	super(e);
	x=e;
    }  
    public String toString(){
	return ("RHIPE[ R Error in "+ RHMRHelper.jobState()+" ] "+x);
    }
}
