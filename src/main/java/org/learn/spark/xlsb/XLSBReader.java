package org.learn.spark.xlsb;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class XLSBReader {
    public static void main(String[] args) throws IOException {
        // Create a Spark session
        SparkSession spark = SparkSession.builder()
                .appName("XLSB Reader")
                .config("spark.master", "local")
                .getOrCreate();

        // Path to the XLSB file
        String filePath = "path/to/your/file.xlsb";

        // Read the XLSB file using Apache POI
        FileInputStream fis = new FileInputStream(filePath);
        Workbook workbook = new SXSSFWorkbook(new XSSFWorkbook(fis));

        List<Row> rows = new ArrayList<>();
        StructType schema = null;

        for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
            Sheet sheet = workbook.getSheetAt(i);
            int rowIndex = 0;

            for (org.apache.poi.ss.usermodel.Row row : sheet) {
                List<Object> values = new ArrayList<>();

                for (Cell cell : row) {
                    switch (cell.getCellType()) {
                        case STRING:
                            values.add(cell.getStringCellValue());
                            break;
                        case NUMERIC:
                            if (DateUtil.isCellDateFormatted(cell)) {
                                values.add(cell.getDateCellValue());
                            } else {
                                values.add(cell.getNumericCellValue());
                            }
                            break;
                        case BOOLEAN:
                            values.add(cell.getBooleanCellValue());
                            break;
                        case FORMULA:
                            values.add(cell.getCellFormula());
                            break;
                        default:
                            values.add(null);
                    }
                }

                // Assume the first row is the header row
                if (rowIndex == 0) {
                    schema = createSchema(values);
                } else {
                    rows.add(RowFactory.create(values.toArray()));
                }
                rowIndex++;
            }
        }

        workbook.close();
        fis.close();

        // Create a DataFrame from the collected data
        Dataset<Row> df = spark.createDataFrame(rows, schema);

        // Show the DataFrame
        df.show();

        // Stop the Spark session
        spark.stop();
    }

    private static StructType createSchema(List<Object> headerValues) {
        List<StructField> fields = new ArrayList<>();
        for (Object headerValue : headerValues) {
            fields.add(DataTypes.createStructField(headerValue.toString(), DataTypes.StringType, true));
        }
        return DataTypes.createStructType(fields);
    }
}
