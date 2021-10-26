package SelfJoin;

import org.junit.jupiter.api.Test;

public class GlobalWindowTests {

    private final GlobalWindowCorrectnessTest correctness;
    private final GlobalWindowDuplicateTest duplicates;

    public GlobalWindowTests(){
        correctness = new GlobalWindowCorrectnessTest();
        duplicates = new GlobalWindowDuplicateTest();
    }

    @Test
    void runCorrectnessTest() throws Exception{

        correctness.testJoinResults();

    }

    @Test
    void runDuplicatesTest() throws Exception{

        duplicates.testDuplicateResults();

    }
}
