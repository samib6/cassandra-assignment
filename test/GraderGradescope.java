import com.gradescope.jh61b.grader.GradedTest;
import com.gradescope.jh61b.grader.GradedTestListenerJSON;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;

import java.io.IOException;
import java.lang.reflect.Method;

@FixMethodOrder(org.junit.runners.MethodSorters.NAME_ASCENDING)
public class GraderGradescope extends GraderSingleServer {

    public static void main(String[] args) throws IOException {

        Class<?> testClass = Grader.class;
        Method[] methods = testClass.getDeclaredMethods();

        for (Method method : methods) {
            if (method.isAnnotationPresent(GradedTest.class)) {
                GradedTest annotation = method.getAnnotation(GradedTest.class);
                double maxScore = annotation.max_score();
                System.out.println("TestNameAndScore: testName=" + method.getName() + " " +
                        "max_score" + "=" + maxScore);
            }
        }

        JUnitCore runner = new JUnitCore();
        runner.addListener(new GradedTestListenerJSON());
        Result r = runner.run(GraderSingleServer.class);
    }
}
