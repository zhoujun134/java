package study.core.tmpclass;

public class StringUtils{
    /**
     * 从拼接的字符串中提取字段
     *
     * @param str       字符串
     * @param delimiter 分隔符
     * @param field     字段
     * @return 字段值
     */
    public static String getFieldFromConcatString(String str, String delimiter, String field){
        String[] fields = str.split(delimiter);
        String result = "0";
        for (String concatField :fields){
            if (concatField.split("=").length == 2) {
                String fieldName = concatField.split("=")[0];
                String fieldValue = concatField.split("=")[1];
                if (fieldName == field) {
                    result = fieldValue;
                }
            }
        }
        return result;
    }

    /**
     * 从拼接的字符串中给字段设置值
     *
     * @param str           字符串
     * @param delimiter     分隔符
     * @param field         字段名
     * @param newFieldValue 新的field值
     * @return 字段值
     */
    public static String setFieldInConcatString(String str, String delimiter, String field, String newFieldValue){
        String[] fields = str.split(delimiter);

        StringBuffer buffer = new StringBuffer("");
        for (String item : fields) {
            String fieldName = item.split("=")[0];
            if (fieldName == field) {
                String concatField = fieldName + "=" + newFieldValue;
                buffer.append(concatField).append("|");
            } else {
                buffer.append(item).append("|");
            }
        }
        return buffer.substring(0, buffer.length() - 1);
    }

}
