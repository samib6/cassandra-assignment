import com.gradescope.jh61b.grader.GradedTest;
import com.gradescope.jh61b.grader.GradedTestListenerJSON;
import org.junit.*;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;

import java.io.IOException;

@FixMethodOrder(org.junit.runners.MethodSorters.NAME_ASCENDING)
public class GraderSingleServerGradescope extends GraderSingleServer {


    public static void main(String[] args) throws IOException {
        JUnitCore runner = new JUnitCore();
        GradedTestListenerJSON gradedTestListenerJSON;
        runner.addListener(gradedTestListenerJSON=new GradedTestListenerJSON());
        Result r = runner.run(GraderSingleServer.class);
        System.exit(0);
    }
}
