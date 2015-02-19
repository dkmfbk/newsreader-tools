package eu.fbk.nwrtools;

import org.junit.Test;

public class URICleanerTest {

    @Test
    public void test() {
        final String input = "http://en.wikinews.org/wiki/China's_military_spending_%20increases_by_7.5%";
        System.out.println(URICleaner.cleanURI(input));
    }

}
