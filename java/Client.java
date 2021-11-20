import java.util.Arrays;

public class Client {
    public static void main(String[] args) {
        long start=System.currentTimeMillis();
        for (String s : DatabaseManipulation.findCourseNameById("CS")) {
            System.out.println(s);
        }
        long end=System.currentTimeMillis();
        System.out.print(end-start);
    }
}