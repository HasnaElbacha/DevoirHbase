package ma.enset.tp_hbase1;

import javax.security.auth.login.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.sql.Connection;

public class App {
    public static final String TABEL_NAME="users";
    public static final String CF_PERSONAL_DATA="personal_data";
    public static final String CF_PROFESIONAL_DATA="profesional_data";
    public static void main(String[] args) {
        Configuration config=HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum","zookeeper");
        config.set("hbase.zookeeper.property.clientPort","2181");
        config.set("hbase.master","hbase-master:16000");

        try {
            Connection connection=ConnectionFactory.createConnection(conf);
            Admin admin=Connection.getAdmin();
            TableName tableName=TableName.valueOf(TABEL_NAME);
            TableDescrptionBuilder builder=TableDescriptionBuiler.newBuilder(tableName);
            builder.setColumnFamilyDescriptionBuilder.of(CF_PERSONAL_DATA);
            builder.setColumnFamilyDescriptionBuilder.of(CF_PROFESIONAL_DATA);
            TableDescription tableDescription=builder.build();
            if(!admin.tableExists(tableName)){
                admin.createTable(tableDescription);
                System.out.println("la table est bien creer");
            }else {
                System.out.println("La table n'est pas bien creer");
            }
            Table table=connection.getTable(tableName);
            Put put=new Put(Bytes.toBytes("11111"));
            put.addColumn(Bytes.toBytes(CF_PERSONAL_DATA),Bytes.toBytes("name"),Bytes.toBytes("Hasna EL BACHA"));
            put.addColumn(Bytes.toBytes(CF_PERSONAL_DATA),Bytes.toBytes("age"),Bytes.toBytes(50));
            put.addColumn(Bytes.toBytes(CF_PERSONAL_DATA),Bytes.toBytes("diplome"),Bytes.toBytes("Ing"));
            table.put(put);
            Result result=
            byte[] name=result.getValue(Bytes.toBytes(CF_PERSONAL_DATA),Bytes.toBytes("age"),Bytes.toBytes(50));

            System.out.println("la ligne est bien inserer");
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
