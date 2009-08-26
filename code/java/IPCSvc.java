// /*
//  * RHIPE - software that integrates Hadoop mapreduce with R
//  *
//  *  This program is free software; you can redistribute it and/or modify
//  *  it under the terms of the GNU General Public License as published by
//  *  the Free Software Foundation; either version 2 of the License, or
//  *  (at your option) any later version.
//  *
//  *  This program is distributed in the hope that it will be useful,
//  *  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  *  GNU General Public License for more details.
//  *
//  *  You should have received a copy of the GNU General Public License
//  *  along with this program; if not, write to the Free Software
//  *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
//  *
//  * Saptarshi Guha sguha@purdue.edu
//  */
// package org.saptarshiguha.rhipe.utils;;
// import java.lang.StringBuilder;
// import java.io.FileInputStream;

// public // class IPCSvc  extends Thread {
// //     private Reporter rptr;
// //     private boolean volatile check;
// //     private volatile FileInputStream read;
// //     public IPCSvc() {
// // 	self.rptr=null;
// // 	self.check=true;
// // 	self.read= new FileInputStream("/tmp/fiko");
// //     }
// //     public void setReporter(Reporter r){
// // 	self.rptr=r;
// //     }
// //     public void stop(){
// // 	self.read=null
// //     }
// //     public void run(){
// // 	byte[] x = new byte[1];
// // 	while(true){
// // 	    if(read!=null) read.read(x); else break;
// // 	    if(x==0x00){
// // 		// A progress check
// // 		self.rprt.progress();
// // 	    }
// // 	}
// //     }
// // }


