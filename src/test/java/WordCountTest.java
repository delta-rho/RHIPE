import org.godhuli.rhipe.mr.WordCount;
import org.junit.Test;

/**
 * User: perk387
 * Date: 1/10/14
 */
public class WordCountTest {
    boolean remote = true;
    @Test
    public void testWordCount() throws  Exception {

        if(remote)
            WordCount.main("/Users/perk387/split-aaaaa /Users/perk387/out".split(" "));
        else
            WordCount.main("/Users/perk387/Projects/sandbox/sample-data-for-canopy/split-aaaaa /Users/perk387/out".split(" "));
    }
}
