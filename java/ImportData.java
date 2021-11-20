import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.file.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class ImportData {
    private static final String pathToJsonFile="C:\\Users\\WLQ\\IdeaProjects\\DB-Project1\\src\\main\\resources\\course_info.json";
    private static final String pathToCsvFile="C:\\Users\\WLQ\\IdeaProjects\\DB-Project1\\src\\main\\resources\\select_course.csv";

    public static <T> List<T> generateList(String className){
        String content=null;
        try {
            content = Files.readString(Path.of(pathToJsonFile));
        } catch (IOException e) {
            e.printStackTrace();
        }
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        Type type=null;
        if(className.equals("Course")){type=new TypeToken<List<Course>>(){}.getType();}
        if(className.equals("Clazz")){type=new TypeToken<List<Clazz>>(){}.getType();}

        return gson.fromJson(content,type);
    }

    /**
     * 不使用PreparedStatement，比较性能差异
     */
    public static void loadCsv1(){
        Connection con=null;
        Statement st=null;
        try {
            con=DatabaseManipulation.getConnection();
            BufferedReader reader = new BufferedReader(new FileReader(pathToCsvFile));
            String line;
            int count=0;
            while((line=reader.readLine())!=null){
                String[] info=line.split(",");
                //导入表Student中的数据
                int studentId=Integer.parseInt(info[3]);
                char gender=info[1].charAt(0);
                char chineseSurname=info[0].charAt(0);
                String chineseGivenName=info[0].substring(1);
                String englishName=info[2].substring(5,info[2].length()-1);
                String translatedEnglishName=info[2].substring(0,4);

                st=con.createStatement();
                String sql="insert into student(id,gender,chinese_surname,chinese_given_name,english_name,translated_english_name)" +
                        "values("+studentId+",'"
                                    +gender+"','"
                            +chineseSurname+"','"
                            +chineseGivenName+"','"
                                +englishName+"','"
                            +translatedEnglishName+"')";
                st.execute(sql);

                //导入表course_selection中的数据
                for (int i = 4; i < info.length; i++) {
                    String courseId=info[i];
                    sql="insert into course_selection(course_id,student_id) " +
                            "values('"+courseId+"',"+studentId+")";
                    st.execute(sql);
                }
                if(count%1000==0){System.out.println(count);}
                count++;
            }
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }finally {
            try {
                if(con!=null)con.setAutoCommit(true);
            } catch (SQLException e) {
                e.printStackTrace();
            }
            DatabaseManipulation.closeResource(con,st);
        }
    }
    /**
     * 不使用批量插入，并且每次for循环都生成一次ps，比较性能差异
     */
    public static void loadCsv2(){
        Connection con=null;
        PreparedStatement ps=null;
        int count=0;
        try {
            con=DatabaseManipulation.getConnection();
            BufferedReader reader = new BufferedReader(new FileReader(pathToCsvFile));
            String line;
            while((line=reader.readLine())!=null){
                String[] info=line.split(",");
                //导入表Student中的数据
                int studentId=Integer.parseInt(info[3]);
                char gender=info[1].charAt(0);
                char chineseSurname=info[0].charAt(0);
                String chineseGivenName=info[0].substring(1);
                String englishName=info[2].substring(5,info[2].length()-1);
                String translatedEnglishName=info[2].substring(0,4);

                String sql="insert into student(id,gender,chinese_surname,chinese_given_name,english_name,translated_english_name) values(?,?,?,?,?,?)";
                ps=con.prepareStatement(sql);
                ps.setObject(1,studentId);
                ps.setObject(2,gender);
                ps.setObject(3,chineseSurname);
                ps.setObject(4,chineseGivenName);
                ps.setObject(5,englishName);
                ps.setObject(6,translatedEnglishName);
                ps.execute();

                //导入表course_selection中的数据
                for (int i = 4; i < info.length; i++) {
                    String courseId=info[i];
                    sql="insert into course_selection(course_id,student_id) values(?,?)";
                    ps=con.prepareStatement(sql);
                    ps.setObject(1,courseId);
                    ps.setObject(2,studentId);
                    ps.execute();
                }
                if(count % 1000==0){System.out.println(count);}
                count++;
            }
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }finally {
            DatabaseManipulation.closeResource(con,ps);
        }
    }
    /**
     * 不使用批量插入，并且在for循环只生成一次ps，比较性能差异
     */
    public static void loadCsv3(){
        Connection con=null;
        PreparedStatement ps1=null;
        PreparedStatement ps2=null;
        String sql1="insert into student(id,gender,chinese_surname,chinese_given_name,english_name,translated_english_name) values(?,?,?,?,?,?)";
        String sql2="insert into course_selection(course_id,student_id) values(?,?)";
        int count=0;
        try {
            con=DatabaseManipulation.getConnection();
            BufferedReader reader = new BufferedReader(new FileReader(pathToCsvFile));
            String line;
            ps1=con.prepareStatement(sql1);
            ps2=con.prepareStatement(sql2);
            while((line=reader.readLine())!=null){
                String[] info=line.split(",");
                //导入表Student中的数据
                int studentId=Integer.parseInt(info[3]);
                char gender=info[1].charAt(0);
                char chineseSurname=info[0].charAt(0);
                String chineseGivenName=info[0].substring(1);
                String englishName=info[2].substring(5,info[2].length()-1);
                String translatedEnglishName=info[2].substring(0,4);

                ps1.setObject(1,studentId);
                ps1.setObject(2,gender);
                ps1.setObject(3,chineseSurname);
                ps1.setObject(4,chineseGivenName);
                ps1.setObject(5,englishName);
                ps1.setObject(6,translatedEnglishName);
                ps1.execute();

                //导入表course_selection中的数据
                for (int i = 4; i < info.length; i++) {
                    String courseId=info[i];
                    ps2=con.prepareStatement(sql2);
                    ps2.setObject(1,courseId);
                    ps2.setObject(2,studentId);
                    ps2.execute();
                }
                if(count % 1000==0){System.out.println(count);}
                count++;
            }
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }finally {
            DatabaseManipulation.closeResource(con,ps1,ps2);
        }
    }
    /**
     * 使用AutoCommit,每提交一次都commit一次，带来开销
     */
    public static void loadCsv4(){
        int batchSize=1000;
        int count1=0;//计数student
        int count2=0;//计数course_selection
        Connection con=null;
        PreparedStatement ps1=null;
        PreparedStatement ps2=null;
        String sql1="insert into student(id,gender,chinese_surname,chinese_given_name,english_name,translated_english_name) values(?,?,?,?,?,?)";
        String sql2="insert into course_selection(course_id,student_id) values(?,?)";
        try {
            con=DatabaseManipulation.getConnection();

            BufferedReader reader = new BufferedReader(new FileReader(pathToCsvFile));
            String line;
            ps1=con.prepareStatement(sql1);
            ps2=con.prepareStatement(sql2);
            while((line=reader.readLine())!=null){
                String[] info=line.split(",");
                //导入表Student中的数据
                int studentId=Integer.parseInt(info[3]);
                char gender=info[1].charAt(0);
                char chineseSurname=info[0].charAt(0);
                String chineseGivenName=info[0].substring(1);
                String englishName=info[2].substring(5,info[2].length()-1);
                String translatedEnglishName=info[2].substring(0,4);

                ps1.setObject(1,studentId);
                ps1.setObject(2,gender);
                ps1.setObject(3,chineseSurname);
                ps1.setObject(4,chineseGivenName);
                ps1.setObject(5,englishName);
                ps1.setObject(6,translatedEnglishName);
                ps1.addBatch();count1++;
                if(count1%batchSize==0){
                    System.out.println(count1);
                    ps1.executeBatch();
                    ps1.clearBatch();
                }

                //导入表course_selection中的数据
                for (int i = 4; i < info.length; i++) {
                    String courseId=info[i];
                    ps2.setObject(1,courseId);
                    ps2.setObject(2,studentId);
                    ps2.addBatch();count2++;
                    if(count2%batchSize==0){
                        ps2.executeBatch();
                        ps2.clearBatch();
                    }
                }
            }
            //最后插入最后剩余的小于batchSize的数据
            if(count1 % batchSize!=0) {
                ps1.executeBatch();
            }
            if(count2 % batchSize!=0){
                ps2.executeBatch();
            }
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }finally {
            DatabaseManipulation.closeResource(con,ps1,ps2);
        }
    }
    /**
     * 不使用AutoCommit,导入完成后一次性提交
     */
    public static void loadCsv5(){
        int batchSize=1000;
        int count1=0;//计数student
        int count2=0;//计数course_selection
        Connection con=null;
        PreparedStatement ps1=null;
        PreparedStatement ps2=null;
        String sql1="insert into student(id,gender,chinese_surname,chinese_given_name,english_name,translated_english_name) values(?,?,?,?,?,?)";
        String sql2="insert into course_selection(course_id,student_id) values(?,?)";
        try {
            con=DatabaseManipulation.getConnection();
            con.setAutoCommit(false);

            BufferedReader reader = new BufferedReader(new FileReader(pathToCsvFile));
            String line;
            ps1=con.prepareStatement(sql1);
            ps2=con.prepareStatement(sql2);
            while((line=reader.readLine())!=null){
                String[] info=line.split(",");
                //导入表Student中的数据
                int studentId=Integer.parseInt(info[3]);
                char gender=info[1].charAt(0);
                char chineseSurname=info[0].charAt(0);
                String chineseGivenName=info[0].substring(1);
                String englishName=info[2].substring(5,info[2].length()-1);
                String translatedEnglishName=info[2].substring(0,4);

                ps1.setObject(1,studentId);
                ps1.setObject(2,gender);
                ps1.setObject(3,chineseSurname);
                ps1.setObject(4,chineseGivenName);
                ps1.setObject(5,englishName);
                ps1.setObject(6,translatedEnglishName);
                ps1.addBatch();count1++;
                if(count1%batchSize==0){
                    System.out.println(count1);
                    ps1.executeBatch();
                    ps1.clearBatch();
                }

                //导入表course_selection中的数据
                for (int i = 4; i < info.length; i++) {
                    String courseId=info[i];
                    ps2.setObject(1,courseId);
                    ps2.setObject(2,studentId);
                    ps2.addBatch();count2++;
                    if(count2%batchSize==0){
                        ps2.executeBatch();
                        ps2.clearBatch();
                    }
                }
            }
            //最后插入最后剩余的小于batchSize的数据
            if(count1 % batchSize!=0) {
                ps1.executeBatch();
            }
            if(count2 % batchSize!=0){
                ps2.executeBatch();
            }
            con.commit();
        } catch (IOException | SQLException e) {
            try {
                con.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            e.printStackTrace();
        }finally {
            DatabaseManipulation.closeResource(con,ps1,ps2);
        }
    }
    public static void loadJson(){
        //1.准备数据
        List<Course> repeatCourses = generateList("Course");
        HashSet<Course> courses = new HashSet<>(repeatCourses);//courses最终导入数据库
        for (Course c : courses) {
            c.standardize();
        }

        List<Clazz> classes=generateList("Clazz");//classes最终导入数据库
        List<ClassList> classLists = new ArrayList<>();//classLists最终导入数据库
        HashMap<String,Integer> teachers = new HashMap<>();//teachers最终导入数据库,String=老师名，int=id
        ArrayList<ClassTeacher> classTeachers = new ArrayList<>();//classTeachers最终导入数据库
        int classIdCount=0;
        int teacherIdCount=32010000;//模拟真实情况
        for (Clazz clazz : classes) {
            //1.设定class自身id
            clazz.setId(++classIdCount);
            //2.生成teachers和classTeachers
            clazz.splitTeacherNames();
            if(clazz.getTeacher()!=null){
                for (String teacherName : clazz.getTeacherNames()) {
                    //如果teachers中已经不包含该教师，则添加入teacher
                    if (!teachers.containsKey(teacherName)) {
                        teachers.put(teacherName, ++teacherIdCount);
                    }
                    //添加每一条class_teacher映射
                    classTeachers.add(new ClassTeacher(classIdCount, teachers.get(teacherName)));
                }
            }
            //3.生成classLists
            for (ClassList cl : clazz.getClassList()) {
                cl.splitClassTime();
                cl.setClassId(classIdCount);
                classLists.add(cl);
            }
        }

        //2.导入数据
        Connection con=DatabaseManipulation.getConnection();
        String sql1="insert into course(id,course_hour,course_credit,name,course_dept,pre_course_names,truth_table) values(?,?,?,?,?,?,?)";
        String sql2="insert into class(id,name,course_id) values(?,?,?)";
        String sql3="insert into class_list(class_id,location,weekday,start_class,end_class,week_list) values(?,?,?,?,?,?)";
        String sql4="insert into teacher(id,name) values(?,?)";
        String sql5="insert into class_teacher(class_id,teacher_id) values(?,?)";
        PreparedStatement ps1=null,ps2=null,ps3=null,ps4=null,ps5=null;
        try {
            con.setAutoCommit(false);
            ps1=con.prepareStatement(sql1);
            ps2=con.prepareStatement(sql2);
            ps3=con.prepareStatement(sql3);
            ps4=con.prepareStatement(sql4);
            ps5=con.prepareStatement(sql5);

            for (Course course : courses) {
                ps1.setString(1,course.getCourseId());
                ps1.setInt(2,course.getCourseHour());
                ps1.setFloat(3,course.getCourseCredit());
                ps1.setString(4,course.getCourseName());
                ps1.setString(5,course.getCourseDept());
                ps1.setObject(6,course.getPreCoursesNames());
                ps1.setObject(7,course.getTruthTable());
                ps1.execute();
            }
            for (Clazz clazz : classes) {
                ps2.setInt(1,clazz.getId());
                ps2.setString(2,clazz.getClassName());
                ps2.setString(3,clazz.getCourseId());
                ps2.execute();
            }
            for (ClassList classList : classLists) {
                ps3.setInt(1,classList.getClassId());
                ps3.setString(2,classList.getLocation());
                ps3.setInt(3,classList.getWeekday());
                ps3.setInt(4,classList.getStartClass());
                ps3.setInt(5,classList.getEndClass());
                ps3.setObject(6,classList.getWeekList());
                ps3.execute();
            }
            for (String s : teachers.keySet()) {
                ps4.setInt(1,teachers.get(s));
                ps4.setString(2,s);
                ps4.execute();
            }
            for (ClassTeacher classTeacher : classTeachers) {
                ps5.setInt(1,classTeacher.classId());
                ps5.setInt(2,classTeacher.teacherId());
                ps5.execute();
            }
            con.commit();
        } catch (SQLException e) {
            try {
                con.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            e.printStackTrace();
        } finally {
            DatabaseManipulation.closeResource(con,ps1,ps2,ps3,ps4,ps5);
        }
    }
}
class Course{
    private String courseId;
    private String prerequisite;//非导入数据
    private int courseHour;
    private float courseCredit;
    private String courseName;
    private String courseDept;
    private String[] preCoursesNames;
    private boolean[][] truthTable;

    /**
     * bug free(几乎可以肯定)
     * <p>生成preCourseName和truthTable，将prerequisite标准化
     */
    public void standardize(){
        if(prerequisite!=null && prerequisite.length()>0){
            //1.去除课程名中的空格，将中文括号改为英文括号
            prerequisite=prerequisite.replace('（','(').replace('）',')')
                    .replace("SUSTech English ","SUSTech_English_")
                    .replace("大学物理 B","大学物理B")
                    .replace("化学原理 A","化学原理A")
                    .replace("化学原理 B","化学原理B")
                    .replace("学术英语 ","学术英语")
                    .replace("生理学与病理生理学 I","生理学与病理生理学I")
                    .replace("线性代数I A","线性代数I-A");

            //2.生成prerequisiteCoursesNames
            String[] temp=prerequisite.split(" ");
            ArrayList<String> courses = new ArrayList<>();
            for (String s : temp) {
                //去除“或者”、“并且”
                if(!s.equals("或者") && !s.equals("并且") && !s.equals("")){
                    //去除单独括号&前后括号，并防止hasDoubleBracket的情况出现
                    while(true){
                        if(s.startsWith("(")){s=s.substring(1);continue;}
                        if(!hasDoubleBracket(s)){
                            s=s.substring(0,s.length()-1);
                            continue;
                        }
                        break;
                    }
                    if(!courses.contains(s)){courses.add(s);}//防止重复添加课程名
                }
            }
            //将courses按名称长度降序排序，目的是避免3.1替换时出现：化学原理→化学原理实验→化学原理实验A(先替换前面的会有bug)的情况
            courses.sort((o1, o2) -> o2.length() - o1.length());
            preCoursesNames=courses.toArray(new String[0]);
            //3.构建truthTable
            //3.1将prerequisite公式化为formulatedPrerequisite
            String formulatedPre =prerequisite;
            for (int i = 0; i < preCoursesNames.length; i++) {
                formulatedPre = formulatedPre.replace(preCoursesNames[i],String.valueOf((char)('A'+i)));//最后替换A
            }
            formulatedPre = formulatedPre.replace(" 并且 ","&")
                                         .replace(" 或者 ","|");
            //3.2生成truthTable
            //3.2.1用逆波兰算法生成后缀表达式
            Stack<Character> stack = new Stack<>();
            StringBuilder sb = new StringBuilder();
            char[] chars = formulatedPre.toCharArray();
            for (char c : chars) {
                switch (c) {
                    case '(' -> stack.push('(');
                    case ')' -> {
                        char top;
                        while ((top = stack.pop()) != '(') {
                            sb.append(top);
                        }
                    }
                    case '&' -> {
                        if (!stack.isEmpty() && stack.peek() == '&') {
                            sb.append(stack.pop());
                        }
                        stack.push('&');
                    }
                    case '|' -> {
                        if (!stack.isEmpty() && stack.peek() == '&') {
                            sb.append(stack.pop());
                        }
                        if (!stack.isEmpty() && stack.peek() == '|') {
                            sb.append(stack.pop());
                        }
                        stack.push('|');
                    }
                    default ->//对应于字母A,B,C...
                            sb.append(c);
                }
            }
            while (!stack.isEmpty()){sb.append(stack.pop());}
            char[] postfix=sb.toString().toCharArray();

            truthTable=new boolean[(int)Math.pow(2, preCoursesNames.length)][1+ preCoursesNames.length];
            for (int i = 0; i < Math.pow(2, preCoursesNames.length); i++) {
                //3.2.2构建每行的真值表的变量取值
                for (int j = 0; j < truthTable[0].length - 1; j++) {
                    truthTable[i][j]=(i&(1<<j))!=0;//判断i的第j+1位是否为1
                }

                //3.2.3用栈计算后缀表达式
                Stack<Boolean> stack2 = new Stack<>();
                for (char c : postfix) {
                    switch (c) {
                        case '|' -> stack2.push(stack2.pop() | stack2.pop());
                        case '&' -> stack2.push(stack2.pop() & stack2.pop());
                        default -> stack2.push(truthTable[i][c - 'A']);
                    }
                }
                truthTable[i][truthTable[0].length - 1]=stack2.pop();
            }
        }
    }

    /**
     *判断String s是否有完整的括号，设计这个方法的目的是防止类似于：
     * <p>“地球物理学基础I(地震学原理)”，“高等数学A(上)”，“生物学技术(一)”等情况的出现
     */
    private boolean hasDoubleBracket(String s){
        char[] chars=s.toCharArray();
        Stack<Boolean> stack = new Stack<>();
        for (char c : chars) {
            switch (c) {
                case '(' -> stack.push(true);
                case ')' -> {
                    if (stack.isEmpty()) {
                        return false;
                    }
                    stack.pop();
                }
            }
        }
        return stack.isEmpty();
    }
    public int hashCode(){
        return Objects.hash(courseId);
    }
    public boolean equals(Object obj){
        if(this==obj){return true;}
        if(!(obj instanceof Course c)){return false;}
        return courseId.equals(c.courseId);
    }

    public String getCourseId() {
        return courseId;
    }
    public int getCourseHour() {
        return courseHour;
    }
    public float getCourseCredit() {
        return courseCredit;
    }
    public String getCourseName() {
        return courseName;
    }
    public String getCourseDept() {
        return courseDept;
    }
    public String[] getPreCoursesNames() {
        return preCoursesNames;
    }
    public boolean[][] getTruthTable() {
        return truthTable;
    }

    public void setCourseId(String courseId) {
        this.courseId = courseId;
    }
    public void setCourseHour(int courseHour) {
        this.courseHour = courseHour;
    }
    public void setCourseCredit(float courseCredit) {
        this.courseCredit = courseCredit;
    }
    public void setCourseName(String courseName) {
        this.courseName = courseName;
    }
    public void setCourseDept(String courseDept) {
        this.courseDept = courseDept;
    }
}
class Clazz {
    private int id;
    private String className;
    private String courseId;
    private ClassList[] classList;//非导入数据
    private String teacher;//非导入数据
    private String[] teacherNames;//非导入数据

    public void splitTeacherNames(){
        if(teacher!=null){teacherNames=teacher.split(",");}
    }

    public ClassList[] getClassList() {
        return classList;
    }
    public void setId(int id) {
        this.id = id;
    }
    public String[] getTeacherNames() {
        return teacherNames;
    }
    public String getTeacher() {
        return teacher;
    }
    public int getId() {
        return id;
    }
    public String getClassName() {
        return className;
    }
    public void setClassName(String className) {
        this.className = className;
    }
    public String getCourseId() {
        return courseId;
    }
    public void setCourseId(String courseId) {
        this.courseId = courseId;
    }
    public void setClassList(ClassList[] classList) {
        this.classList = classList;
    }
    public void setTeacher(String teacher) {
        this.teacher = teacher;
    }
}
class ClassList {
    private int[] weekList;
    private String location;
    private String classTime;//非导入数据
    private int classId;
    private int weekday;
    private int startClass;
    private int endClass;

    /**
     * 将classTime解耦为startClass和endClass
     */
    public void splitClassTime(){
        startClass=Character.getNumericValue(classTime.charAt(0));
        endClass=Integer.parseInt(classTime.substring(2));
    }

    public int[] getWeekList() {
        return weekList;
    }
    public void setWeekList(int[] weekList) {
        this.weekList = weekList;
    }
    public String getLocation() {
        return location;
    }
    public void setLocation(String location) {
        this.location = location;
    }
    public String getClassTime() {
        return classTime;
    }
    public void setClassTime(String classTime) {
        this.classTime = classTime;
    }
    public int getClassId() {
        return classId;
    }
    public void setClassId(int classId) {
        this.classId = classId;
    }
    public int getWeekday() {
        return weekday;
    }
    public void setWeekday(int weekday) {
        this.weekday = weekday;
    }
    public int getStartClass() {
        return startClass;
    }
    public int getEndClass() {
        return endClass;
    }
}
record ClassTeacher(int classId, int teacherId){}