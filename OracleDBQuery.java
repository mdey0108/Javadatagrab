import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class OracleDBQueryMultiThreaded {
    private static final int THREAD_COUNT = 10; // Adjust as per your CPU cores and database performance

    public static void main(String[] args) {
        String dbUrl = "jdbc:oracle:thin:@oprodexd.scan.ocwen.com:1503/cdmsprd.world";
        String dbUsername = "mahesh";
        String dbPassword = "root";
        String inputCsv = "input.csv";
        String outputCsv = "output.csv";

        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            List<String> inputValues = readInputCsv(inputCsv);

            ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
            List<Future<List<String>>> futures = new ArrayList<>();

            int batchSize = (int) Math.ceil((double) inputValues.size() / THREAD_COUNT);
            for (int i = 0; i < THREAD_COUNT; i++) {
                int start = i * batchSize;
                int end = Math.min(start + batchSize, inputValues.size());
                if (start < end) {
                    List<String> batch = inputValues.subList(start, end);
                    futures.add(executor.submit(new QueryTask(dbUrl, dbUsername, dbPassword, batch)));
                }
            }

            List<String> outputData = new ArrayList<>();
            outputData.add("user_key_1,pg_id,user_key_5");
            for (Future<List<String>> future : futures) {
                outputData.addAll(future.get());
            }

            executor.shutdown();
            writeOutputCsv(outputCsv, outputData);
            System.out.println("Data processing completed. Output written to: " + outputCsv);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

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

    private static void writeOutputCsv(String outputCsv, List<String> data) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputCsv))) {
            for (String line : data) {
                writer.write(line);
                writer.newLine();
            }
        }
    }
}

class QueryTask implements Callable<List<String>> {
    private final String dbUrl;
    private final String dbUsername;
    private final String dbPassword;
    private final List<String> inputValues;

    public QueryTask(String dbUrl, String dbUsername, String dbPassword, List<String> inputValues) {
        this.dbUrl = dbUrl;
        this.dbUsername = dbUsername;
        this.dbPassword = dbPassword;
        this.inputValues = inputValues;
    }

    @Override
    public List<String> call() {
        List<String> outputData = new ArrayList<>();
        String query = "SELECT user_key_1, pg_id, user_key_5 FROM cdms.optimg01 WHERE user_key_3=1098 AND user_key_1 = ?";
        
        try (Connection connection = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
             PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            
            for (String value : inputValues) {
                preparedStatement.setString(1, value);
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        String user_key_1 = resultSet.getString("user_key_1");
                        String pg_id = resultSet.getString("pg_id");
                        String user_key_5 = resultSet.getString("user_key_5");
                        outputData.add(user_key_1 + "," + pg_id + "," + user_key_5);
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return outputData;
    }
}
