import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class OracleDBQuery {
    public static void main(String[] args) {
        // Database connection details // change as per database details username and passwords 
        String dbUrl = "jdbc:oracle:thin:@oprodexd.scan.ocwen.com:1503/cdmsprd.world";
        String dbUsername = "mahesh";
        String dbPassword = "root";

        // CSV file paths
        String inputCsv = "input.csv"; // Path to input CSV file
        String outputCsv = "output.csv"; // Path to output CSV file

        Connection connection = null;

        try {
            // Load Oracle JDBC Driver
            Class.forName("oracle.jdbc.driver.OracleDriver");

            // Connect to the database
            connection = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);

            // Read input CSV file
            List<String> inputValues = readInputCsv(inputCsv);

            // Write output CSV file
            writeOutputCsv(connection, inputValues, outputCsv);

            System.out.println("Data processing completed. Output written to: " + outputCsv);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Close the connection
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    // Read input CSV file and extract the first column values
    private static List<String> readInputCsv(String inputCsv) throws IOException {
        List<String> inputValues = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(inputCsv))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] columns = line.split(",");
                if (columns.length > 0) {
                    inputValues.add(columns[0].trim());
                }
            }
        }
        return inputValues;
    }

    // Execute query and write output to CSV
    private static void writeOutputCsv(Connection connection, List<String> inputValues, String outputCsv) throws SQLException, IOException {

        //change in below query as per requirement 
        String query = "SELECT user_key_1,pg_id,user_key_5 FROM cdms.optimg01 WHERE user_key_3=1098 and user_key_1 = ?";
        try (PreparedStatement preparedStatement = connection.prepareStatement(query);
             BufferedWriter writer = new BufferedWriter(new FileWriter(outputCsv))) {

            // Write header to output CSV // change as per requirement 
            writer.write("user_key_1,pg_id,user_key_5"); // Update with your actual column names
            writer.newLine();

            for (String value : inputValues) {
                preparedStatement.setString(1, value);
                ResultSet resultSet = preparedStatement.executeQuery();

                while (resultSet.next()) {
                    // Extract data from the result set //add rows as below as per required to save details in output 
                    String user_key_1 = resultSet.getString("user_key_1"); // Replace with actual column name
                    String pg_id = resultSet.getString("pg_id"); // Replace with actual column name
                    String user_key_5 = resultSet.getString("user_key_5"); // Replace with actual column name

                    // Write to output CSV // add the rows you want to save in output as below 
                    writer.write(user_key_1 + "," + pg_id + "," + user_key_5);
                    writer.newLine();
                }
            }
        }
    }
}
