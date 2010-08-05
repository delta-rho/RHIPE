package org.godhuli.rhipe.Matrix;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;

public class BlockedMatrixWriter {
    private static final byte _signature = (byte)0xaa;
    private int _ncolumns, _nrows;
    private int _blockcolumns, _blockrows,_blockarea;
    private FSDataOutputStream _idx;
    public BlockedMatrixWriter(int ncols,int nrows,
			       int blockrows, int blockcols,
			       FileSystem fs,Path outputfolder) 
	throws BadStructureException, IOException{
	if( (ncols % blockcols !=0) )
	    throw new BadStructureException("Number of Columns should be multiple of Block rows");
	if((nrows % blockrows !=0))
	    throw new BadStructureException("Number of Rows should be multiple of Block cols");
	// if( ncols & (ncols-1) !=0)
	//     throw new BadStructureException("Number of columns must be a power of 2");
	// if( nrows & (nrows-1) !=0)
	//     throw new BadStructureException("Number of rows must be a power of 2");
	
	_ncolumns = ncols;
	_nrows = nrows;
	_blockrows = blockrows;
	_blockcolumns = blockcols;
	_idx = fs.create(outputfolder);
	_blockarea = _blockcolumns*_blockrows;
    }

    /*
     * Currently proof of concept: number of columns 
     * and rows must be a power of 2
     */

    public  BlockedMatrixWriter(int ncols, int nrows, FileSystem fs, Path outputfolder)
	throws BadStructureException,IOException{
	this(ncols, nrows,4096,4096,fs,outputfolder);
    }

    public void intialize() throws IOException {
	_idx.writeByte(_signature);
	_idx.writeInt(_ncolumns);
	_idx.writeInt(_nrows);
	_idx.writeInt(_blockcolumns);
	_idx.writeInt(_blockrows);
    }

    public void close() throws IOException{
	_idx.close();
    }
    /*
     * the length of i must be equal to blockcols*blockrows
     * blocks are added in row major order.
     */

    public void addBlock(double[] data) throws BadStructureException,IOException {
	if( data.length != _blockarea) throw new BadStructureException("Data length is not equal to block area");
	for(int j=0;j<data.length;j++) _idx.writeDouble(data[j]);
    }
}