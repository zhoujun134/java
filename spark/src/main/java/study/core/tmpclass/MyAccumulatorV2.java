package study.core.tmpclass;

import org.apache.spark.util.AccumulatorV2;
public class MyAccumulatorV2  extends AccumulatorV2<String, String>{
    private String result = "use0=0|user1=0|user2=0|user3=0";

    /**
     * 当 AccumulatorV2 中存在类似数据不存在这种问题时，是否结束程序。
     * @return
     */
    @Override
    public boolean isZero() {
        return true;
    }

    /**
     * 拷贝一个新的 AccumulatorV2
     * @return
     */
    @Override
    public AccumulatorV2<String, String> copy()  {
        MyAccumulatorV2 myAccumulatorV2 = new MyAccumulatorV2();
        myAccumulatorV2.result = this.result;
        return myAccumulatorV2;
    }

    /**
     * 重置 AccumulatorV2 中的数据
     */
    @Override
    public void reset() {
        result = "user0=0|user1=0|user2=0|user3=0";
    }

    /**
     * 操作数据累加方法实现
     * @param v
     */
    @Override
    public void add(String v) {
        String v1 = result;
        String v2 = v;
        //    log.warn("v1 : " + v1 + " v2 : " + v2)
        if (v1 != null && v2 != null) {
            String newResult = "";
            // 从v1中，提取v2对应的值，并累加
            String oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2);
            if (oldValue != null) {
                String newValue = (Integer.parseInt(oldValue) + 1) + "";
                newResult = StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue));
            }
            result = newResult;
        }
    }

    /**
     * 合并数据
     * @param other
     */
    @Override
    public void merge(AccumulatorV2<String, String> other) {
        if(other == this) result = other.value();
        else throw new UnsupportedOperationException();
    }

    /**
     * AccumulatorV2 对外访问的数据结果
     * @return
     */
    @Override
    public String value() {
        return result;
    }
}
