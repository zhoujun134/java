import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by zhoujun on 2017/9/9.
 */
public class Test {
    public static void main(String[] args){
        System.out.println("Hello world");
        List<String> list = new ArrayList<String>(Arrays.asList("Hello", " world", " I", " am ", "zhoujun"));

        list.stream()
                .filter(s -> s.length() > 3)
                .forEach(System.out::println);


    }
}
