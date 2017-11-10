package learm.forclass.experiment;

import learm.forclass.utils.HBaseUtils;

import java.io.IOException;

/**
 * Created by zhoujun on 17-11-9.
 */
public class Main {

    public static void main(String[] args) throws IOException {
//        HBaseUtils.dropTable("student");
//        HBaseUtils.createTable("student", "info,course");
//        HBaseUtils.createTable("course", "info,student");
        // 插入数据
//        for(int i=0; i<20; i++){
//            System.out.println("start input data.....");
//            HBaseUtils.putDataH("student","15400"+i, "info", "name","st-"+i);
//            if(i%2==0) {
//                HBaseUtils.putDataH("student","15400"+i, "info", "age",""+(i%10+10));
//                HBaseUtils.putDataH("student", "15400"+i, "info", "sex", "male");
//            }
//            else{
//                HBaseUtils.putDataH("student","15400"+i, "info", "age",""+(i%10+10));
//                HBaseUtils.putDataH("student","15400"+i, "info", "sex","female");
//            }
//            for(int j=0; j<(int)(Math.random()*10); j++){
//                HBaseUtils.putDataH("student","15400"+i, "course", "c" + j,"c"+j);
//            }
//        }

//        for(int i=0; i<9; i++){
//            System.out.println("start input data.....");
////            HBaseUtils.putDataH("course","c"+i, "info", "title","title-"+i);
////            HBaseUtils.putDataH("course","c"+i, "info", "introduction","introduction-"+i);
////            HBaseUtils.putDataH("course","c"+i, "info", "teacher_id","teacher_id-"+i);
//
//        }


//        System.out.println("input data.....");
//        HBaseUtils.putDataH("course","c0", "student", "t0","154000");
//        HBaseUtils.putDataH("course","c0", "student", "t1","154001");
//        HBaseUtils.putDataH("course","c0", "student", "t2","154002");
//        HBaseUtils.putDataH("course","c0", "student", "t3","154003");
//        HBaseUtils.putDataH("course","c0", "student", "t4","154004");
//        HBaseUtils.putDataH("course","c0", "student", "t5","154005");
//        HBaseUtils.putDataH("course","c0", "student", "t6","154006");
//        HBaseUtils.putDataH("course","c0", "student", "t7","154007");
//        HBaseUtils.putDataH("course","c0", "student", "t8","154008");
//        HBaseUtils.putDataH("course","c0", "student", "t9","154009");
//        HBaseUtils.putDataH("course","c0", "student", "t10","1540010");
//        HBaseUtils.putDataH("course","c0", "student", "t11","1540011");
//        HBaseUtils.putDataH("course","c0", "student", "t12","1540012");
//        HBaseUtils.putDataH("course","c0", "student", "t13","1540013");
//        HBaseUtils.putDataH("course","c0", "student", "t14","1540014");
//        HBaseUtils.putDataH("course","c0", "student", "t15","1540015");
//        HBaseUtils.putDataH("course","c0", "student", "t16","1540016");
//        HBaseUtils.putDataH("course","c0", "student", "t17","1540017");
//        HBaseUtils.putDataH("course","c0", "student", "t18","1540018");
//        HBaseUtils.putDataH("course","c0", "student", "t19","1540019");
//        HBaseUtils.putDataH("course","c1", "student", "t0","154000");
//        HBaseUtils.putDataH("course","c2", "student", "t0","154000");
//        HBaseUtils.putDataH("course","c1", "student", "t1","154001");
//        HBaseUtils.putDataH("course","c2", "student", "t1","154001");
//        HBaseUtils.putDataH("course","c3", "student", "t1","154001");
//        HBaseUtils.putDataH("course","c4", "student", "t1","154001");
//        HBaseUtils.putDataH("course","c1", "student", "t10","1540010");
//        HBaseUtils.putDataH("course","c1", "student", "t12","1540012");
//        HBaseUtils.putDataH("course","c2", "student", "t12","1540012");
//        HBaseUtils.putDataH("course","c3", "student", "t12","1540012");
//        HBaseUtils.putDataH("course","c4", "student", "t12","1540012");
//        HBaseUtils.putDataH("course","c5", "student", "t12","1540012");
//        HBaseUtils.putDataH("course","c1", "student", "t13","1540013");
//        HBaseUtils.putDataH("course","c2", "student", "t13","1540013");
//        HBaseUtils.putDataH("course","c3", "student", "t13","1540013");
//
//        HBaseUtils.putDataH("course","c1", "student", "t14","1540014");
//        HBaseUtils.putDataH("course","c1", "student", "t15","1540015");
//        HBaseUtils.putDataH("course","c2", "student", "t15","1540015");
//        HBaseUtils.putDataH("course","c3", "student", "t15","1540015");
//        HBaseUtils.putDataH("course","c4", "student", "t15","1540015");
//        HBaseUtils.putDataH("course","c1", "student", "t16","1540016");
//        HBaseUtils.putDataH("course","c2", "student", "t16","1540016");
//        HBaseUtils.putDataH("course","c1", "student", "t17","1540017");
//        HBaseUtils.putDataH("course","c2", "student", "t17","1540017");
//        HBaseUtils.putDataH("course","c3", "student", "t17","1540017");
//        HBaseUtils.putDataH("course","c1", "student", "t19","1540019");
//        HBaseUtils.putDataH("course","c2", "student", "t18","1540018");
//        HBaseUtils.putDataH("course","c1", "student", "t18","1540018");
//        HBaseUtils.putDataH("course","c2", "student", "t3","1540013");
//        HBaseUtils.putDataH("course","c3", "student", "t3","1540013");
//        HBaseUtils.putDataH("course","c2", "student", "t4","154004");
//        HBaseUtils.putDataH("course","c1", "student", "t4","154004");
//        HBaseUtils.putDataH("course","c3", "student", "t5","154005");
//        HBaseUtils.putDataH("course","c2", "student", "t5","154005");
//        HBaseUtils.putDataH("course","c1", "student", "t5","154005");
//        HBaseUtils.putDataH("course","c1", "student", "t6","154006");
//        HBaseUtils.putDataH("course","c1", "student", "t7","154007");
//        HBaseUtils.putDataH("course","c1", "student", "t8","154008");
//        HBaseUtils.putDataH("course","c3", "student", "t9","154009");
//        HBaseUtils.putDataH("course","c2", "student", "t9","154009");
//        HBaseUtils.putDataH("course","c1", "student", "t9","154009");


//        EXP01.fromStudentIdGetDate("154000");

//        EXP01.fromCourseIdGetDate("c1");
//        EXP01.fromTeacherIdGetDate("teacher_id-2");

        EXP01.getMinInCourseStudent();
    }
}
