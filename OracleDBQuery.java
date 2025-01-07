import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class OracleDBQuery {
    public static void main(String[] args) {
        // Database connection details
        String dbUrl = "jdbc:oracle:thin:@oprodexd.scan.ocwen.com:1503";
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
        String query = "SELECT * FROM your_table_name WHERE your_column_name = ?";
        try (PreparedStatement preparedStatement = connection.prepareStatement(query);
             BufferedWriter writer = new BufferedWriter(new FileWriter(outputCsv))) {

            // Write header to output CSV
            writer.write("Column1,Column2,Column3"); // Update with your actual column names
            writer.newLine();

            for (String value : inputValues) {
                preparedStatement.setString(1, value);
                ResultSet resultSet = preparedStatement.executeQuery();

                while (resultSet.next()) {
                    // Extract data from the result set
                    String column1 = resultSet.getString("column1"); // Replace with actual column name
                    String column2 = resultSet.getString("column2"); // Replace with actual column name
                    String column3 = resultSet.getString("column3"); // Replace with actual column name

                    // Write to output CSV
                    writer.write(column1 + "," + column2 + "," + column3);
                    writer.newLine();
                }
            }
        }
    }
}
