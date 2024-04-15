package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Main {
    public static final String TABEL_NAME="users";
    public static final String CF_PERSONAL_DATA="personal_data";
    public static final String CF_PROFESIONAL_DATA="profesional_data";
    private static Object Bytes;

    public static void main(String[] args) {
        Configuration config= HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum","zookeeper");
        config.set("hbase.zookeeper.property.clientPort","2181");
        config.set("hbase.master","hbase-master:16000");

        try {
            Connection connection= ConnectionFactory.createConnection(config);
            Admin admin=connection.getAdmin();
            TableName tableName=TableName.valueOf(TABEL_NAME);
            TableDescriptorBuilder builder=TableDescriptorBuilder.newBuilder(tableName);
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_PERSONAL_DATA));
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_PROFESIONAL_DATA));
            TableDescriptor tableDescription=builder.build();
            if(!admin.tableExists(tableName)){
                admin.createTable(tableDescription);
                System.out.println("la table est bien creer");
            }else {
                System.out.println("La table n'est pas bien creer");
            }
            Table table=connection.getTable(tableName);
            Put put=new Put(org.apache.hadoop.hbase.util.Bytes.toBytes("11111"));
            put.addColumn(org.apache.hadoop.hbase.util.Bytes.toBytes(CF_PERSONAL_DATA),org.apache.hadoop.hbase.util.Bytes.toBytes("name"),org.apache.hadoop.hbase.util.Bytes.toBytes("Hasna EL BACHA"));
            put.addColumn(org.apache.hadoop.hbase.util.Bytes.toBytes(CF_PERSONAL_DATA),org.apache.hadoop.hbase.util.Bytes.toBytes("age"),org.apache.hadoop.hbase.util.Bytes.toBytes(50));
            put.addColumn(org.apache.hadoop.hbase.util.Bytes.toBytes(CF_PERSONAL_DATA),org.apache.hadoop.hbase.util.Bytes.toBytes("diplome"),org.apache.hadoop.hbase.util.Bytes.toBytes("Ing"));
            table.put(put);
            //Result result=table.get(get);
            //byte[] name=result.getValue(Bytes.toBytes(CF_PERSONAL_DATA),Bytes.toBytes("age"),Bytes.toBytes(50));

            System.out.println("la ligne est bien inserer");
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}