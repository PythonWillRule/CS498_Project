package cs498.final_project;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import java.io.FileOutputStream;
import cs498.final_project.dicom.DicomFileDataPreprocessor;
import cs498.final_project.utils.ByteUtilities;
import cs498.final_project.utils.Converter;
import cs498.final_project.dicom.DicomFilePersister;
import cs498.final_project.dicom.DicomObject;
import java.util.ArrayList;
/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
        assertTrue( true );
    }
    
    public  AppTest(){
    	super();
    }
     
    public static void main(String[] args) throws Exception {
    	String dicomFilePath = "./data/2da20d108988b468cbb3205e640e47ca.dcm";
    	byte[] bytes = ByteUtilities.dicomFileAsByteArray(dicomFilePath);
    	System.out.println(bytes.length);
    	DicomFileDataPreprocessor dfdp = new DicomFileDataPreprocessor(bytes);   //this is how the mapper should instantiate the dicom pre-processor
    	byte[] newBytes = dfdp.execute();  //this is how the mapper should invoke pre-processing of a dicom file.
    	System.out.println(newBytes.length);
    	FileOutputStream fileOuputStream = new FileOutputStream("./output/2da20d108988b468cbb3205e640e47ca.dcm");
        fileOuputStream.write(newBytes);
        fileOuputStream.close();
        Attributes dataset = ByteUtilities.byteArrayToDicomAttributes(newBytes);  //use this as part of instantiating a DicomFilePersister instance
        System.out.println("Attributes from updated DICOM File: " + dataset);  
        System.out.println("Pixel Data length from updated DICOM File: " + Converter.getPixelData(dataset).length);
        Attributes metadata = ByteUtilities.byteArrayToDicomMetadata(newBytes);  //use this as part of instantiating a DicomFilePersister instance
        System.out.println("Meta Data  from updated DICOM File: " + metadata);  
        System.out.println("Transfer Syntax: " + metadata.getString(Tag.TransferSyntaxUID, null));
        double pixelSpacingX = dataset.getDouble(Tag.PixelSpacing, 0, 0.32);
        double pixelSpacingY = dataset.getDouble(Tag.PixelSpacing, 1, 0.32);
        System.out.println("Pixel Spacing X Coordinate:" + pixelSpacingX);
        System.out.println("Pixel Spacing Y Coordinate:" + pixelSpacingY);
        System.out.println("Patient Name: " + dataset.getString(Tag.PatientName));
        System.out.println("Patient Birthdate: " + dataset.getDate(Tag.PatientBirthDate));
        System.out.println("Scan Acquistion: " + dataset.getDate(Tag.SeriesDate));
        System.out.println("Series: " + dataset.getInt(Tag.SeriesNumber, 0));
        String patientId = dataset.getString(Tag.PatientID);
        Integer instanceNumber = dataset.getInt(Tag.InstanceNumber, 1);
        String rowId = patientId+instanceNumber;
        System.out.println("Hbase row id: "+ rowId);
        DicomFilePersister persister = new DicomFilePersister(newBytes);
        persister.insert();
        DicomFilePersister persister2 = new DicomFilePersister();
        DicomObject dicomObject = persister2.reteiveByRowId(patientId,instanceNumber);
        System.out.println("Retreived Dicom Object from Hbase: " + dicomObject);
        byte[] retrievedBytes = dicomObject.getDicomObject();
        Attributes retrievedDataset = ByteUtilities.byteArrayToDicomAttributes(retrievedBytes);
        String retrievedPatientId = retrievedDataset.getString(Tag.PatientID);
        int retrievedInstanceNumber = retrievedDataset.getInt(Tag.InstanceNumber, -9999);
        System.out.println("Dicom File instance: " + retrievedInstanceNumber + " retrieved from Hbase with Patient Id: " + retrievedPatientId);
        System.out.println("****** Testing Scan *****");
        ArrayList<DicomObject> dicomObjects = persister2.scan(retrievedPatientId, retrievedInstanceNumber, retrievedPatientId, 200);
        for (DicomObject dicomObject2 : dicomObjects){
        	System.out.println("Dicom Object: " + dicomObject2);
        }
        
        
        
    }
    
    
    
}
