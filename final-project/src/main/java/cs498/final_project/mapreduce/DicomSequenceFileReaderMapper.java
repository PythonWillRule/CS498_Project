package cs498.final_project.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import cs498.final_project.dicom.DicomFileDataPreprocessor;
import cs498.final_project.dicom.DicomFilePersister;
import cs498.final_project.utils.ByteUtilities;
import cs498.final_project.utils.Converter;

/**
 * Mapper class of the MapReduce package.
 * It just writes the input key-value pair to the context
 * 
 * @author Raman
 *
 */
public class DicomSequenceFileReaderMapper extends Mapper<BytesWritable,BytesWritable,Text,Text> {
	private static final String SUCCESS_INSERT = "Successful insert into Hbase.";
	private static final String UNSUCCESSFUL_INSERT = "Unsuccessful insert into HBase";
	
	/**
	 * This is the map function, it does not perform much functionality.
	 * It only writes key and value pair to the context
	 * which will then be written into the sequence file.
	 */
	@Override
    protected void map(BytesWritable key, BytesWritable value,Context context) throws IOException, InterruptedException {
        boolean isSuccessfulInsert = false;
        String msg = String.valueOf(Float.MAX_VALUE);
        Attributes dataset = null;
        DicomFilePersister persister = null;
        String patientId = null;
        Integer instanceNumber = null;
        
       //To Do insert logic to pre-process dicom file and to insert it into Hbase;
        try{
        DicomFileDataPreprocessor preprocessor = new DicomFileDataPreprocessor(value.copyBytes());
        byte[] newBytes = preprocessor.execute();
        dataset = ByteUtilities.byteArrayToDicomAttributes(newBytes);  
        patientId = dataset.getString(Tag.PatientID);
        instanceNumber = dataset.getInt(Tag.InstanceNumber, 1);
        String  rowId = patientId+instanceNumber;
        persister = new DicomFilePersister(newBytes);
        isSuccessfulInsert = persister.insert();
        }
        catch(Exception ex){
        	isSuccessfulInsert = false;
        	RuntimeException rte =  new RuntimeException(ex);
        	rte.printStackTrace();
        	throw rte;
        	
        }
        
        Text rowIdText = new Text("patient id: " + patientId + " instance number "+ instanceNumber);
        if (isSuccessfulInsert){
        	msg = SUCCESS_INSERT;
        }
        else{
        	msg = UNSUCCESSFUL_INSERT;
        }
        
		
		context.write(rowIdText, new Text(msg));                
    }
	
}
