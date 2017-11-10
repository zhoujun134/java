package learm.forclass.experiment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

/**
 * Created by zhoujun on 17-11-9.
 */
public class EXP01 {

    private static Configuration conf = HBaseConfiguration.create();


    private static Connection connection = null;

    /**
     * 根据学号student_id查询学生选课编号course_id和名称title
     * @param studentId 学生——id
     * @return
     * @throws IOException
     */

    public static HashMap<String, String> fromStudentIdGetDate(String studentId) throws IOException{

        HashMap<String, String> results = new HashMap<>();
        connection = ConnectionFactory.createConnection(conf);
        Table student = connection.getTable(TableName.valueOf("student"));

        Table course = connection.getTable(TableName.valueOf("course"));

        Get get = new Get(Bytes.toBytes(studentId));
        get.addFamily(Bytes.toBytes("course"));

        Result result = student.get(get);

        for(Cell cell: result.rawCells()){
            Get getCourse = new Get(Bytes.toBytes(Bytes.toString(cell.getQualifier()).trim()));
            getCourse.addFamily(Bytes.toBytes("info"));
            System.out.println("courseId: " + Bytes.toString(cell.getQualifier()));
            Result result2 = course.get(getCourse);
            System.out.println("title: " + Bytes.toString(result2.getValue(Bytes.toBytes("info"), Bytes.toBytes("title"))));
            results.put(Bytes.toString(cell.getQualifier()),
                    Bytes.toString(result2.getValue(Bytes.toBytes("info"),
                            Bytes.toBytes("title"))));
        }
        return results;
    }


    /**
     * 根据课程号course_id查询选课学生学号student_id和姓名name
     * @param courseId 课程号id
     * @return
     * @throws IOException
     */
    public static HashMap<String, String> fromCourseIdGetDate(String courseId) throws IOException{

        HashMap<String, String> results = new HashMap<>();
        connection = ConnectionFactory.createConnection(conf);
        Table student = connection.getTable(TableName.valueOf("student"));

        Table course = connection.getTable(TableName.valueOf("course"));

        Get get = new Get(Bytes.toBytes(courseId));
        get.addFamily(Bytes.toBytes("student"));

        Result result = course.get(get);

        for(Cell cell: result.rawCells()){
            Get getStudent = new Get(result.getValue(Bytes.toBytes("student"), cell.getQualifier()));
            getStudent.addFamily(Bytes.toBytes("info"));
            System.out.println("StudentId: " + Bytes.toString(result.getValue(Bytes.toBytes("student"), cell.getQualifier())));

            Result result2 = student.get(getStudent);

            System.out.println("name: " + Bytes.toString(result2.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))));

            results.put(Bytes.toString(result.getValue(Bytes.toBytes("student"), cell.getQualifier())),
                    Bytes.toString(result2.getValue(Bytes.toBytes("info"),
                            Bytes.toBytes("name"))));
        }
        return results;

    }



    /**
     * 根据教员号teacher_id查询该教员所上课程编号course_id和名称title
     * @param teacherId teacher_Id
     * @return
     * @throws IOException
     */
    public static HashMap<String, String> fromTeacherIdGetDate(String teacherId) throws IOException {

        HashMap<String, String> results = new HashMap<>();
        connection = ConnectionFactory.createConnection(conf);

        Table courseTable = connection.getTable(TableName.valueOf("course"));
        ValueFilter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(Bytes.toBytes(teacherId)));
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner scanner = courseTable.getScanner(scan);
        for(Result result: scanner){
            for (Cell cell : result.rawCells()) {
                Get get = new Get(cell.getRow());
                get.addFamily(Bytes.toBytes("info"));
                Result result1 = courseTable.get(get);
                System.out.println("courseId: " + Bytes.toString(cell.getRow()) + ", title: " +
                        Bytes.toString(result1.getValue(
                                Bytes.toBytes("info"),Bytes.toBytes("title"))));
                results.put(Bytes.toString(cell.getRow()),
                        Bytes.toString(result1.getValue(
                                Bytes.toBytes("info"),Bytes.toBytes("title"))));
            }
        }

        return results;

    }

    /**
     * 上课最多的学生
     * @return
     * @throws IOException
     */
    public static Student getMaxInCourseStudent() throws IOException{
        Student student = new Student();

        HashMap<String, Integer> info = new HashMap<>();
        connection = ConnectionFactory.createConnection(conf);

        Table table = connection.getTable(TableName.valueOf("student"));

        Scan scan = new Scan();

        ResultScanner scanner = table.getScanner(scan);
        int max = 0;
        String studentId = "";
        for(Result result: scanner){
            int tempCount = 0;
            Get get = new Get(result.getRow());
            get.addFamily(Bytes.toBytes("course"));
            for(Cell cell: table.get(get).rawCells()){
                tempCount++;
            }
            if(max < tempCount){
                max = tempCount;
                studentId = Bytes.toString(result.getRow());
            }
        }

        System.out.println("id: " + studentId + " max: " + max);

        table.close();
        connection.close();

        return student;
    }


    /**
     * 上课最少的学生
     * @return
     * @throws IOException
     */
    public static Student getMinInCourseStudent() throws IOException{
        Student student = new Student();

        HashMap<String, Integer> info = new HashMap<>();
        connection = ConnectionFactory.createConnection(conf);

        Table table = connection.getTable(TableName.valueOf("student"));

        Scan scan = new Scan();

        ResultScanner scanner = table.getScanner(scan);
        int min = 1;
        String studentId = "";
        for(Result result: scanner){
            int tempCount = 0;
            Get get = new Get(result.getRow());
            get.addFamily(Bytes.toBytes("course"));
            for(Cell cell: table.get(get).rawCells()){
                tempCount++;
            }
            if(min >= tempCount){
                min = tempCount;
                studentId = Bytes.toString(result.getRow());
            }
        }

        System.out.println("id: " + studentId + " min: " + min);

        table.close();
        connection.close();

        return student;
    }


}
