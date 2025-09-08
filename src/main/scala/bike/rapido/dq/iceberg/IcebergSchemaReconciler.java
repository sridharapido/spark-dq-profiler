package bike.rapido.dq.iceberg;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergSchemaReconciler {
    private final Table icebergTableRef;
    private final Dataset<Row> resultDfRef;
    private Boolean isTest = false;

    private List<String> columnsToAddInIcebergTable = null;
    private List<String> columnsToAddInDf = null;

    private static final Logger logger = LoggerFactory.getLogger(IcebergSchemaReconciler.class);

    public IcebergSchemaReconciler(Table icebergTableRef, Dataset<Row> resultDfRef) {
        this.icebergTableRef = icebergTableRef;
        this.resultDfRef = resultDfRef;
    }

    public void forTest() { this.isTest = true; }

    private List<String> getColumnsToAddInIcebergTable() {
        if (this.columnsToAddInIcebergTable == null) {
            Schema currentSchema = this.icebergTableRef.schema();
            List<String> dfColumns = Arrays.asList(this.resultDfRef.columns());
            List<String> tableColumns = getColumnNamesFromSchema(currentSchema);
            this.columnsToAddInIcebergTable = dfColumns.stream().filter(col -> !tableColumns.contains(col)).collect(Collectors.toList());
        }
        return this.columnsToAddInIcebergTable;
    }

    private List<String> getColumnsToAddInDf() {
        if (this.columnsToAddInDf == null) {
            Schema currentSchema = this.icebergTableRef.schema();
            List<String> dfColumns = Arrays.asList(this.resultDfRef.columns());
            List<String> tableColumns = getColumnNamesFromSchema(currentSchema);
            this.columnsToAddInDf = tableColumns.stream().filter(col -> !dfColumns.contains(col)).collect(Collectors.toList());
        }
        return this.columnsToAddInDf;
    }

    public void updateTableSchema() {
        List<String> columnsToAdd = this.getColumnsToAddInIcebergTable();
        UpdateSchema updateSchema = this.icebergTableRef.updateSchema();
        for (String colName : columnsToAdd) {
            StructField colToAdd = findColsInSchema(this.resultDfRef, colName);
            logger.info("Updating table ("+ this.icebergTableRef.name() + ") schema. Adding column " + colToAdd.name() + ", " + colToAdd.dataType() + " to table definition");
            updateSchema = updateSchema.addColumn(colToAdd.name(), SparkSchemaUtil.convert(colToAdd.dataType()));
        }
        if (!columnsToAdd.isEmpty() && !this.isTest) {
            updateSchema.commit();
            try {
                logger.info("Waiting for iceberg schema update commit to finish");
                Thread.sleep(120000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public Dataset<Row> getDfWithUpdatedSchema() {
        Dataset<Row> clonedDf = this.resultDfRef.toDF(this.resultDfRef.columns());
        List<String> columnsPresentInTableButNotInDf = this.getColumnsToAddInDf();
        for (String colName : columnsPresentInTableButNotInDf) {
            clonedDf = clonedDf.withColumn(colName, functions.lit(null).cast("double"));
        }
        return clonedDf;
    }

    private StructField findColsInSchema(Dataset<Row> df, String colName) { return df.schema().apply(colName); }

    private List<String> getColumnNamesFromSchema(Schema newSchema) {
        return newSchema.columns().stream().map(col -> col.name()).collect(Collectors.toList());
    }
}

