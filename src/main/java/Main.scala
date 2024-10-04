import hbase.HBaseConnectionFactory
import org.apache.hadoop.hbase.client.{ColumnFamilyDescriptorBuilder, Connection, TableDescriptorBuilder}
import org.apache.hadoop.hbase.{NamespaceDescriptor, TableName}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}

object Main {
  val spark = SparkSession.builder().getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

//  private def createReplaceTable(connection: Connection, inpTableName: String): Unit = {
//    val admin = connection.getAdmin
//    val tableName = TableName.valueOf(inpTableName)
//
//    if (admin.tableExists(tableName)) {
//      println(s"Table $tableName exists. Deleting...")
//      admin.disableTable(tableName)
//      admin.deleteTable(tableName)
//    }
//
//    if (!admin.tableExists(tableName)) {
//      val tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName)
//
//      val departmentFamily = ColumnFamilyDescriptorBuilder.newBuilder("department".getBytes).build()
//      val salaryFamily = ColumnFamilyDescriptorBuilder.newBuilder("salary".getBytes).build()
//      val titleFamily = ColumnFamilyDescriptorBuilder.newBuilder("title".getBytes).build()
//
//      tableDescriptorBuilder.setColumnFamily(departmentFamily)
//      tableDescriptorBuilder.setColumnFamily(salaryFamily)
//      tableDescriptorBuilder.setColumnFamily(titleFamily)
//
//      admin.createTable(tableDescriptorBuilder.build())
//      println("Bảng đã được tạo thành công!")
//    } else {
//      println("Bảng đã tồn tại.")
//    }
//    admin.close()
//  }

  private def createDataFrameAndPutToHBase(): Unit = {
    val jdbcUrl = "jdbc:mysql://localhost:3306/employees"
    val jdbcProperties = new java.util.Properties()
    jdbcProperties.setProperty("user", "root")
    jdbcProperties.setProperty("password", "123456")

    val employeesDF = spark.read
      .jdbc(jdbcUrl, "employees", jdbcProperties)

    employeesDF.show()
  }

  def main(args: Array[String]): Unit = {
//    val hbaseConnection = HBaseConnectionFactory.createConnection()
//    val tableName = "employees"
//    createReplaceTable(hbaseConnection, tableName)
    createDataFrameAndPutToHBase()
  }
}
