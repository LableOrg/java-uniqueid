package org.lable.util.uniqueid.zookeeper;

import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.lable.util.uniqueid.zookeeper.ZooKeeperHelper.pathParts;

public class ZooKeeperHelperTest {
    @Test
    public void pathPartsTest() {
        final String INPUT_A = "/znode";
        final List<String> RESULT_A = pathParts(INPUT_A);
        assertThat(RESULT_A.size(), is(1));
        assertThat(RESULT_A.get(0), is("/znode"));

        final String INPUT_B = "/znode/nested";
        List<String> RESULT_B = pathParts(INPUT_B);
        assertThat(RESULT_B.size(), is(2));
        assertThat(RESULT_B.get(0), is("/znode"));
        assertThat(RESULT_B.get(1), is("/znode/nested"));
    }
}