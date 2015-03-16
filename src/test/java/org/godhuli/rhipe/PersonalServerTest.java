package org.godhuli.rhipe;

import junit.framework.Assert;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * User: perk387
 * Date: 2/10/14
 */
public class PersonalServerTest extends PersonalServerBase {
    private PersonalServer personalServer;
    @Before
    public void setUp() throws Exception {
        personalServer = new PersonalServer();
        personalServer.run(0);
        personalServer.getConf().set("fs.default.name", "hdfs://localhost:9000");
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testGetFU() throws Exception {
        Assert.assertNotNull(personalServer.getFU());
    }

    @Test
    public void testGetConf() throws Exception {
        Assert.assertNotNull(personalServer.getConf());
    }

    @Test
    public void testRhmropts() throws Exception {
        Assert.assertNotNull(personalServer.rhmropts());
    }

    @Test
    public void testRhls() throws Exception {
        byte[] results = personalServer.rhls("/tmp",1);
       Assert.assertNotNull(results);
    }

    @Test
    public void testRhdel() throws Exception {
        //todo having issues here with the default file system running in "standalone mode" have to override the file system
        final Path tmpPath = new Path("hdfs://localhost:9000/tmp/junit-test");
        Assert.assertTrue(tmpPath.getFileSystem(personalServer.getConf()).mkdirs(tmpPath));
        personalServer.rhdel(tmpPath.toString());
    }

    @Test
    public void testRhget() throws Exception {
        final Path tmpPath = new Path("hdfs://localhost:9000/tmp/junit-test2");
        Assert.assertTrue(tmpPath.getFileSystem(personalServer.getConf()).mkdirs(tmpPath));
        //todo: why not use this instead of what is in rhget?
        tmpPath.getFileSystem(personalServer.getConf()).copyToLocalFile(true,tmpPath, new Path("/tmp/junit-test2"));
//        personalServer.rhget(tmpPath.toString(),"hdfs://localhost:9000/tmp/copy");
//        personalServer.rhdel(tmpPath.toString());
    }

    @Test
    public void testRhput() throws Exception {

    }

    @Test
    public void testRhstatus() throws Exception {

    }

    @Test
    public void testRhjoin() throws Exception {

    }

    @Test
    public void testRhkill() throws Exception {

    }

    @Test
    public void testRhex() throws Exception {

    }

    @Test
    public void testInitializeCaches() throws Exception {

    }

    @Test
    public void testInitializeMapFile() throws Exception {

    }

    @Test
    public void testRhgetkeys2() throws Exception {

    }

    @Test
    public void testCacheStatistics() throws Exception {

    }

    @Test
    public void testReadTextFile() throws Exception {

    }

    @Test
    public void testRun() throws Exception {

    }
}
