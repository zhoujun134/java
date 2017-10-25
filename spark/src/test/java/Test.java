import java.math.BigDecimal;
import java.util.Scanner;

/**
 * Created by zhoujun on 2017/9/22.
 */
public class Test {
    public static void main(String[] args){
        double charge = 0; //收费
        int weigth = new Scanner(System.in).nextInt(); //重量
        if(weigth <= 10) charge = 3.5f;
        else charge = 3.5 + (weigth%10 * 0.75);
        // 格式化为两位小数
        charge = new BigDecimal(charge).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
        System.out.println("费用为： " + charge);

    }
}
