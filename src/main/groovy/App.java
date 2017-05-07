

import java.io.File;
import org.dcm4che3.io.DicomInputStream;
import org.dcm4che3.io.DicomInputStream.IncludeBulkData;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import java.io.IOException;
import CS498.final_project.utils.ByteUtilities;
import CS498.final_project.utils.Converter;



/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "attempting to read dicom file" );
        String dicomFilePath = args[0];
        DicomInputStream din = null;
        Attributes attributes = null;
        byte[] rawPixelData = null;
        short[] convertedPixelData = null;
        float rescaleSlope = 1;
        float rescaleIntercept = 1;
        try {
            din = new DicomInputStream(new File(dicomFilePath));
            System.out.println("Read Dicom File: " + dicomFilePath);
            attributes = din.getFileMetaInformation();
            System.out.println("Meta Data Attributes: " + attributes);
            System.out.println("bulk data descriptor: " + din.getBulkDataDescriptor());
            System.out.println("Bulk Data Directory: "  + din.getBulkDataDirectory());
            System.out.println("Bulk Data File Prefix: "  + din.getBulkDataFilePrefix() + " Bulk Data File Suffix: " + din.getBulkDataFileSuffix());
            System.out.println("Included Bulk Data: " + din.getIncludeBulkData());
            System.out.println("Bulk Data Files: " + din.getBulkDataFiles().toArray());
            System.out.println("URI: " + din.getURI());
            din.setIncludeBulkData(IncludeBulkData.URI);
            Attributes dataset = din.readDataset(-1, -1);
            System.out.println("Attributes: " + dataset);
            System.out.println("Contains Raw Pixel Data as Bytes: " + dataset.contains(Tag.PixelData));
            rawPixelData = dataset.getBytes(Tag.PixelData);
            if (rawPixelData != null){
            	System.out.println("Raw Pixel Data Length in Bytes: " +  rawPixelData.length);
            }
            rescaleSlope = dataset.getFloat(Tag.RescaleSlope, 1);
            rescaleIntercept = dataset.getFloat(Tag.RescaleIntercept, 1);
            
            System.out.println("Rescale Slope: " + rescaleSlope + " Rescale Intercept: " + rescaleIntercept);
            
            float impaePosition1 = dataset.getFloat(Tag.ImagePositionPatient, 1, 0);
            float impaePosition2 = dataset.getFloat(Tag.ImagePositionPatient, 2, 0);
            float impaePosition3 = dataset.getFloat(Tag.ImagePositionPatient, 3, 0);
            
            System.out.println("ImagePostions 1 -3:   " + impaePosition1 + " : " + impaePosition2 + " : " + impaePosition3);
            
            float sliceLocation = dataset.getFloat(Tag.SliceLocation, 0);
            
            System.out.println("Slice Location: " + sliceLocation);
            
            float sliceThickness = Converter.calculateSliceThickness(new Float(impaePosition2), new Float(sliceLocation), new Float((impaePosition2 + 10.25)), new Float((sliceLocation + 10.25)));
            
            System.out.println("Simulated slice thickness: " + sliceThickness);
            
            int instanceNumber = dataset.getInt(Tag.InstanceNumber, 0);
            
            System.out.println("Instance number: " + instanceNumber);
             
            convertedPixelData = ByteUtilities.byteArrayToShortArray(rawPixelData);
            if (convertedPixelData != null){
            	System.out.println("Coverted Pixel Data Length in Shorts: " +  convertedPixelData.length);
            }
            
            
            
            System.out.print("Displaying first 100 no zero pixels as shorts \n");
            
            int count = 0;
            
            for (int i = 0; i < convertedPixelData.length; i++){
            	short pixel = convertedPixelData[i];
            	
            	if (pixel > 0){
            		System.out.println("pixel[" + i + "] value is: " + pixel);
            		count++;
            	}
            	if (count == 100){
            		break;
            	}
            }
            
    
            
            for (int i = 0; i < convertedPixelData.length; i++){
            	short hounsefieldUnit = Converter.toHounsefieldUnit(convertedPixelData[i], rescaleSlope, rescaleIntercept);
            	convertedPixelData[i] = hounsefieldUnit;
            }
            
            int count2 = 0;
            System.out.println("Displaying the first 100 hounsefield Units > 0" );
            for (int i = 0; i < convertedPixelData.length; i++){
            	short pixel = convertedPixelData[i];     
            	if (pixel > 0){
            		System.out.println("pixel[" + i + "] as hounsefield unit is: " + pixel);
            		count2++;
            	}
            	if (count2 > 100){
            		break;
            	}

            }
            
        }
        catch (IOException e) {
            e.printStackTrace();
            return;
        }
        finally {
            try {
                din.close();
            }
            catch (IOException ignore) {
            }
        }
        
    }
}
