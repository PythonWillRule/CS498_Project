package cs498.final_project.hbase;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

public class CreateTables {
	public static void main(String[] args) throws IOException {

		// Instantiating configuration class
		Configuration con = HBaseConfiguration.create();
		// Instantiating HbaseAdmin class
		HBaseAdmin admin = new HBaseAdmin(con);	
		
		// TODO
		// Instantiating table descriptor class
		TableName patients = TableName.valueOf("patient");
		//admin.disableTable(patients);
		HTableDescriptor patientsTableDescriptor = new HTableDescriptor(patients);
		patientsTableDescriptor.addFamily(new HColumnDescriptor("dicomfile"));
		admin.createTable(patientsTableDescriptor);
		//admin.enableTable(patients);
		HTableDescriptor[] tables = admin.listTables();
		boolean patientsTableCreated = false;

		for (int i = 0; i < tables.length; i++) {
			if (Bytes.equals(patients.getName(), tables[i].getName())) {
				patientsTableCreated = true;
			}

		}
		if (patientsTableCreated == true) {
			System.out.println("patient table was created");
		} else {
			System.out.println("patient table was not created");
		}

	}
}
