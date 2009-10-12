// package org.godhuli.rhipe;
// import org.apache.mina.core.session.IdleStatus;
// import org.apache.mina.core.service.IoAcceptor;
// import org.apache.mina.filter.codec.ProtocolCodecFilter;
// import org.apache.mina.filter.codec.textline.TextLineCodecFactory;
// import org.apache.mina.filter.logging.LoggingFilter;
// import org.apache.mina.filter.logging.LogLevel;
// import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
// import java.io.IOException;
// import org.apache.hadoop.conf.Configuration;
// import java.net.InetSocketAddress;

// public class RHIPEClientDispatcher {
//     public static void main( String[] args ) throws Exception
//     {
	
// 	int PORT  = Integer.parseInt(args[0]);
// 	NioSocketAcceptor acceptor = new NioSocketAcceptor();
// 	RHIPEClientHandler handler;
// 	handler = new RHIPEClientHandler();
// 	acceptor.getFilterChain().
// 	    addLast("protocol",
// 		    new ProtocolCodecFilter(new CommandCodecFactory()));
// 	acceptor.setReuseAddress(true);
// 	acceptor.setHandler(handler);
// 	acceptor.bind( new InetSocketAddress(PORT));
	
//     }
// }