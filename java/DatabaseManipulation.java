import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

public class DatabaseManipulation {
    public static Connection getConnection(){
        InputStream is = ImportData.class.getClassLoader().getResourceAsStream("jdbc.properties");
        Properties pros = new Properties();
        Connection con = null;
        try {
            pros.load(is);

            String user=pros.getProperty("user");
            String password=pros.getProperty("password");
            String url=pros.getProperty("url");
            String driverClass=pros.getProperty("driverClass");

            Class.forName(driverClass);
            con = DriverManager.getConnection(url,user,password);
        } catch (IOException|ClassNotFoundException|SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Connect Successfully");
        return con;
    }
    public static void closeResource(Connection con, Statement... statements){
        if(statements!=null){
            try {
                for (Statement statement : statements) {
                    statement.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if(con!=null){
            try {
                con.close();
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Close resource successfully");
    }

    public static String[] findCourseNameById(String s){
        Connection con=getConnection();
        PreparedStatement ps=null;
        ResultSet rs=null;
        ArrayList<String> names=null;
        String sql="select name from course where id like '"+s+"%'";
        try {
            ps=con.prepareStatement(sql);
            rs=ps.executeQuery();
            names = new ArrayList<>();
            while(rs.next()){
                names.add(rs.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            closeResource(con,ps);
            try {
                if(rs!=null)rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        assert names != null;
        return names.toArray(new String[0]);
    }
}