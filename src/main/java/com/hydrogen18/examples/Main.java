package com.hydrogen18.examples;

// Generic Avro dependencies
import org.apache.avro.Schema;

// Hadoop stuff
import org.apache.hadoop.fs.Path;

// Generic Parquet dependencies
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.ParquetWriter;

// Avro->Parquet dependencies
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.avro.AvroParquetWriter;

public final class Main {
  public static void main(String[] args){
    Schema avroSchema = UserRank.getClassSchema();
    MessageType parquetSchema = new AvroSchemaConverter().convert(avroSchema);    
    
    UserRank dataToWrite[] = new UserRank[]{
      new UserRank(1, 3),
      new UserRank(2, 0),
      new UserRank(3, 100)
    };

  
    Path filePath = new Path("./example.parquet");
    int blockSize = 1024;
    int pageSize = 65535;
    try(
      AvroParquetWriter parquetWriter = new AvroParquetWriter(
        filePath, 
        avroSchema, 
        CompressionCodecName.SNAPPY, 
        blockSize, 
        pageSize)
      ){
      for(UserRank obj : dataToWrite){
        parquetWriter.write(obj); 
      }
    }catch(java.io.IOException e){
      System.out.println(String.format("Error writing parquet file %s", e.getMessage()));
      e.printStackTrace();
    }
      
  }
}
