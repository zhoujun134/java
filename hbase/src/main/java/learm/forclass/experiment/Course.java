package learm.forclass.experiment;

import java.util.List;

/**
 * Created by zhoujun on 17-11-9.
 */
public class Course {
    private String tilte;
    private String introduction;
    private String teacherId;
    private List<Student> students;

    public Course() {
    }

    public Course(String tilte, String introduction, String teacherId, List<Student> students) {
        this.tilte = tilte;
        this.introduction = introduction;
        this.teacherId = teacherId;
        this.students = students;
    }

    public String getTilte() {
        return tilte;
    }

    public void setTilte(String tilte) {
        this.tilte = tilte;
    }

    public String getIntroduction() {
        return introduction;
    }

    public void setIntroduction(String introduction) {
        this.introduction = introduction;
    }

    public String getTeacherId() {
        return teacherId;
    }

    public void setTeacherId(String teacherId) {
        this.teacherId = teacherId;
    }

    public List<Student> getStudents() {
        return students;
    }

    public void setStudents(List<Student> students) {
        this.students = students;
    }

    @Override
    public String toString() {
        return "Course{" +
                "tilte='" + tilte + '\'' +
                ", introduction='" + introduction + '\'' +
                ", teacherId='" + teacherId + '\'' +
                ", students=" + students +
                '}';
    }
}
