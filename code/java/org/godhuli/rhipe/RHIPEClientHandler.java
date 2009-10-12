// package org.godhuli.rhipe;
// import org.apache.mina.core.service.IoHandlerAdapter;
// import org.apache.mina.core.session.IoSession;
// import org.slf4j.Logger;  
// import org.slf4j.LoggerFactory; 
// import org.apache.hadoop.conf.Configuration;

// public class RHIPEClientHandler extends IoHandlerAdapter {
//     private final static String FUKEY = "FILEUTIL";

    
//     public void exceptionCaught(IoSession session, Throwable cause) throws Exception {
// 	cause.printStackTrace();
//     }

//     public RHIPEClientHandler() throws Exception {
//     }
//     public void sessionCreated(IoSession session) throws Exception {
// 	Configuration cfg=new Configuration();
// 	FileUtils fileutil = new FileUtils(cfg);
// 	session.setAttribute(FUKEY, fileutil);
//     }
//     public void sessionClosed(IoSession session) throws Exception { System.out.println("RHIPE:Bye Bye");   }

//     public void messageReceived(IoSession session, Object message)  {
// 	CommandExecution request = (CommandExecution) message;
// 	FileUtils fu = (FileUtils)session.getAttribute(FUKEY);
// 	try{
// 	    request.perform(session, fu);
// 	}catch(Exception e){
// 	    e.printStackTrace();
// 	}
//     }
// }