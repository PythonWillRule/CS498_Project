package cs498.final_project.utils;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.BulkData;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.VR;

public class Converter {
	
  public static final Logger logger = 	Logger.getLogger(Converter.class);
	
  public static short toHounsefieldUnit(short pixel, float slope, float intercept){
	  
	  short val = Short.MIN_VALUE;
	  if (pixel == -2000){
		  val = 0;
		  logger.info("Pixel value " + pixel + " falls out of scan boundary.  Pixel value set to " + val + " before hounsefield conversion ");
	  }
	  else{
		  val = (short)((pixel*slope) + intercept);
	  }
	  return val;
  }
  
  
  public static final short normanlizeHounsefieldUnit(short hounsefieldUnit, int lowerBoundThreshhold, int upperBoundThreshhold){
	  //for the purpose of this study we are interested in hounsefield units between -1000 and 400.  Any hounsefield unit value lower than -1000 is considered air and anything 
	  //and any hounsefield unit value greater than 400 is considered bone.
	  int defaultLowerBoundsThreshhold  = -1000;
	  int defaultUpperBoundsThreshold = 400;
	  
	  if (upperBoundThreshhold <= lowerBoundThreshhold){
		  logger.warn("Upper bound for hounsefield unit normalization must be greater than the lower bound for hounsefield unit normalization.  Using default upper and lower bounds.");
		  upperBoundThreshhold = defaultUpperBoundsThreshold;
		  lowerBoundThreshhold = defaultLowerBoundsThreshhold;
	  }
	  
	  short val = Short.MIN_VALUE;
	  
	  if (hounsefieldUnit < lowerBoundThreshhold){
		  val = 0;
	  }
	  else if (hounsefieldUnit > upperBoundThreshhold) {
		  val = 1;
	  }
	  else{
		  val = hounsefieldUnit;
	  }
	  
	  return val;
  }
  
  public static float calculateSliceThickness(Float firstImagePositionPatientValue2, Float firstSliceLocation, Float secondImagePositionPatientValue2, Float secondSliceLocation){
	  //slice thickness is missing from the the DICOM files and is necessary to perform resampling.  As a result, we estimate the 
	  //slice thickness for the patient's series of images (e.g., slices) by taking the absolute value of the difference of the ImagePositionPatientValue2 value
	  //between two contiguous images (slices).  If ImagePositionValue2 is missing from either of the slices we use the SliceLocation value between the 
	  //two contiguous images.
	  float val = 1;
	  if (firstImagePositionPatientValue2 == null || firstImagePositionPatientValue2.isNaN()|| secondImagePositionPatientValue2 == null || secondImagePositionPatientValue2.isNaN()){
		  val = Math.abs(secondSliceLocation.floatValue() - firstSliceLocation.floatValue());
	  }
	  else{
		  val = Math.abs(secondImagePositionPatientValue2.floatValue() - firstImagePositionPatientValue2.floatValue());
	  }
	  return val;
  }
  
  
  public static short zeroCenter(short pixel_value, double pixel_mean){
	  
	  return (short)(pixel_value - pixel_mean);
  }
  
  public static  byte[] getPixelData(Attributes attributes) throws IOException{
  	byte[] rawPixelData = null;
  	VR.Holder vr = new VR.Holder();
  	Object pixelData = attributes.getValue(Tag.PixelData, vr);
      if (pixelData instanceof byte[]) {
         logger.info("Pixel data is instanceof byte[].");
         rawPixelData = (byte[])pixelData;
      } else if (pixelData instanceof BulkData){
      	logger.info("Pixel data is instanceof BulkData.");
      	rawPixelData = ((BulkData)pixelData).toBytes(VR.OW, false);
      }
      return rawPixelData;
  }
  
 
  
  
}
