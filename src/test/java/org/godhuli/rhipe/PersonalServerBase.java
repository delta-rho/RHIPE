package org.godhuli.rhipe;

import org.apache.hadoop.conf.Configuration;

/**
 * User: perk387
 * Date: 2/10/14
 */
public class PersonalServerBase {
    protected String localDataDir = "src/test/resources";
    private String zonfFile = "zonf";
    private Configuration conf;

    protected Configuration getHadoopConfiguration() throws Exception{
        if(conf == null){
            conf = new Configuration();
        }
        return conf;
    }

    protected String getZonfFile() throws Exception {
        return localDataDir + "/" + zonfFile;
    }
}
