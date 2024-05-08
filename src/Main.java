import java.sql.SQLException;

public class Main {
    public static void main(String[] args) throws SQLException {
        PostgreConnection db = new PostgreConnection();
        db.connect_to_db();
    }
}