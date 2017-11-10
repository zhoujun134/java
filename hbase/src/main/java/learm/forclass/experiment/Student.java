package learm.forclass.experiment;

import java.util.List;

/**
 * Created by zhoujun on 17-11-9.
 */
public class Student {
    private String name;
    private String age;
    private String sex;
    private List<Course> course;

    public Student() {
    }

    public Student(String name, String age, String sex, List<Course> course) {
        this.name = name;
        this.age = age;
        this.sex = sex;
        this.course = course;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public List<Course> getCourse() {
        return course;
    }

    public void setCourse(List<Course> course) {
        this.course = course;
    }
}
