import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class PostgreConnection {
    public Connection connect_to_db() throws SQLException {
        Connection con = null;
        try {
            Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "platano612");

            conn.setAutoCommit(false);
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

            Statement stmt1 = conn.createStatement();

            // Create Tables
            String sqlProduct = "CREATE TABLE IF NOT EXISTS Product (" +
                    "prodid VARCHAR(5)," +
                    "pname VARCHAR(30)," +
                    "price DECIMAL(10, 2)," +
                    "PRIMARY KEY (prodid))";
            stmt1.executeUpdate(sqlProduct);

            String sqlDepot = "CREATE TABLE IF NOT EXISTS Depot (" +
                    "depid VARCHAR(5)," +
                    "addr VARCHAR(30)," +
                    "volume INT," +
                    "PRIMARY KEY (depid))";
            stmt1.executeUpdate(sqlDepot);

            String sqlStock = "CREATE TABLE IF NOT EXISTS Stock (" +
                    "prodid VARCHAR(5)," +
                    "depid VARCHAR(5)," +
                    "quantity INT," +
                    "PRIMARY KEY (prodid, depid)," +
                    "FOREIGN KEY (prodid) REFERENCES Product(prodid)," +
                    "FOREIGN KEY (depid) REFERENCES Depot(depid))";
            stmt1.executeUpdate(sqlStock);

            // Insert values into Product
            String sqlProductInsert1 = "INSERT INTO Product (prodid, pname, price) VALUES ('p1', 'tape', 2.5)";
            stmt1.executeUpdate(sqlProductInsert1);
            String sqlProductInsert2 = "INSERT INTO Product (prodid, pname, price) VALUES ('p2', 'v', 250)";
            stmt1.executeUpdate(sqlProductInsert2);
            String sqlProductInsert3 = "INSERT INTO Product (prodid, pname, price) VALUES ('p3', 'vcr', 80)";
            stmt1.executeUpdate(sqlProductInsert3);

            // Insert values into Depot
            String sqlDepotInsert1 = "INSERT INTO Depot (depid, addr, volume) VALUES ('d1', 'New York', 9000)";
            stmt1.executeUpdate(sqlDepotInsert1);
            String sqlDepotInsert2 = "INSERT INTO Depot (depid, addr, volume) VALUES ('d2', 'Syracuse', 6000)";
            stmt1.executeUpdate(sqlDepotInsert2);
            String sqlDepotInsert3 = "INSERT INTO Depot (depid, addr, volume) VALUES ('d3', 'New York', 2000)";
            stmt1.executeUpdate(sqlDepotInsert3);

            // Insert values into Stock
            String sqlStockInsert1 = "INSERT INTO Stock (prodid, depid, quantity) VALUES ('p1', 'd1', 1000) ON CONFLICT DO NOTHING";
            stmt1.executeUpdate(sqlStockInsert1);
            String sqlStockInsert2 = "INSERT INTO Stock (prodid, depid, quantity) VALUES ('p1', 'd2', -100) ON CONFLICT DO NOTHING";
            stmt1.executeUpdate(sqlStockInsert2);
            String sqlStockInsert3 = "INSERT INTO Stock (prodid, depid, quantity) VALUES ('p1', 'd1', 1200) ON CONFLICT DO NOTHING";
            stmt1.executeUpdate(sqlStockInsert3);
            String sqlStockInsert4 = "INSERT INTO Stock (prodid, depid, quantity) VALUES ('p3', 'd1', 3000) ON CONFLICT DO NOTHING";
            stmt1.executeUpdate(sqlStockInsert4);
            String sqlStockInsert5 = "INSERT INTO Stock (prodid, depid, quantity) VALUES ('p3', 'd3', 2000) ON CONFLICT DO NOTHING";
            stmt1.executeUpdate(sqlStockInsert5);
            String sqlStockInsert6 = "INSERT INTO Stock (prodid, depid, quantity) VALUES ('p2', 'd2', 1500) ON CONFLICT DO NOTHING";
            stmt1.executeUpdate(sqlStockInsert6);
            String sqlStockInsert7 = "INSERT INTO Stock (prodid, depid, quantity) VALUES ('p2', 'd1', -400) ON CONFLICT DO NOTHING";
            stmt1.executeUpdate(sqlStockInsert7);
            String sqlStockInsert8 = "INSERT INTO Stock (prodid, depid, quantity) VALUES ('p2', 'd2', 2000) ON CONFLICT DO NOTHING";
            stmt1.executeUpdate(sqlStockInsert8);

            // 4 Transactions

            String updateProductName = "UPDATE Product SET pname = 'pp1' WHERE prodid = 'p1'";
            stmt1.executeUpdate(updateProductName);

            String updateProductNameInStock = "UPDATE Stock SET prodid = 'pp1' WHERE prodid = 'p1'";
            stmt1.executeUpdate(updateProductNameInStock);

            String updateDepotName = "UPDATE Depot SET addr = 'Chicago' WHERE depid = 'd1'";
            stmt1.executeUpdate(updateDepotName);

            String updateDepotNameInStock = "UPDATE Stock SET depid = 'dd1' WHERE depid = 'd1'";
            stmt1.executeUpdate(updateDepotNameInStock);

            String insertNewProduct = "INSERT INTO Product (prodid, pname, price) VALUES ('p100', 'cd', 5)";
            stmt1.executeUpdate(insertNewProduct);

            String insertStockForNewProduct = "INSERT INTO Stock (prodid, depid, quantity) VALUES ('p100', 'd2', 50)";
            stmt1.executeUpdate(insertStockForNewProduct);

            String insertNewDepot = "INSERT INTO Depot (depid, addr, volume) VALUES ('d100', 'Chicago', 100)";
            stmt1.executeUpdate(insertNewDepot);

            String insertStockForNewDepot = "INSERT INTO Stock (prodid, depid, quantity) VALUES ('p1', 'd100', 100)";
            stmt1.executeUpdate(insertStockForNewDepot);

            conn.commit();
            stmt1.close();
            conn.close();
            return con;
        } catch (SQLException e) {
            System.out.println("An exception was thrown");
            e.printStackTrace();
        }
        return con;
    }
}