// package org.godhuli.rhipe;
// import org.slf4j.Logger;  
// import org.slf4j.LoggerFactory; 
// import org.apache.mina.core.session.IoSession;
// import org.apache.mina.core.buffer.IoBuffer;
// import org.apache.mina.filter.codec.ProtocolDecoder;
// import org.apache.mina.filter.codec.ProtocolEncoder;
// import org.apache.mina.filter.codec.ProtocolDecoderOutput;
// import org.apache.mina.filter.codec.ProtocolCodecFactory;
// import org.apache.mina.filter.codec.CumulativeProtocolDecoder;
// import org.apache.mina.filter.codec.ProtocolEncoderAdapter;
// import org.apache.mina.filter.codec.ProtocolEncoderOutput;



// class CommandExecutionDecoder extends CumulativeProtocolDecoder {
//     protected boolean doDecode(IoSession session, IoBuffer in, ProtocolDecoderOutput out) throws Exception {
//         if (in.prefixedDataAvailable(4)) {
//             int opcmd = in.getInt();
// 	    int serialized_length = in.getInt();

//             byte[] bytes = new byte[serialized_length];
//             in.get(bytes);
//             CommandExecution cmd = new CommandExecution(opcmd,bytes);
//             out.write(cmd);
// 	    System.out.println(cmd);
//             return true;
//         } else {
//             return false;
//         }
//     }
// }
// class CommandResponseEncoder extends ProtocolEncoderAdapter {
//     private Logger logger = LoggerFactory.getLogger(this.getClass());
//     public void encode(IoSession session, Object message, ProtocolEncoderOutput out) throws Exception {
// 	CommandExecution resp = (CommandExecution) message;
//         IoBuffer buffer = IoBuffer.allocate(resp.getLength(), false);
//         buffer.setAutoExpand(true);
// 	switch(resp.getType()){
// 	case 0x0:
// 	case 0x1:
// 	    buffer.put(resp.getType());
// 	    buffer.putInt(resp.getLength());
// 	    buffer.put(resp.getBytes());
// 	    buffer.flip();
// 	    out.write(buffer);
// 	    break;
// 	case 0x2:
// 	    buffer.put(resp.getType());
// 	    buffer.flip();
// 	    out.write(buffer);
// 	    break;
// 	}
//     }
// }


// public class CommandCodecFactory implements ProtocolCodecFactory {
//     private ProtocolDecoder decoder;
//     private ProtocolEncoder encoder;
//     private Logger logger = LoggerFactory.getLogger(this.getClass());
//     public CommandCodecFactory() {
// 	decoder = new CommandExecutionDecoder();
// 	encoder = new CommandResponseEncoder();
//     }
//     public ProtocolEncoder getEncoder(IoSession ioSession) throws Exception {
//         return encoder;
//     }

//     public ProtocolDecoder getDecoder(IoSession ioSession) throws Exception {
//         return decoder;
//     }
// }









