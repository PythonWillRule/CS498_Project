package cs498.final_project.dicom;

import org.dcm4che3.io.DicomInputStream;
import org.dcm4che3.io.DicomOutputStream;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.VR;
import cs498.final_project.utils.ByteUtilities;
import cs498.final_project.utils.Converter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import org.apache.log4j.Logger;

public class DicomFileDataPreprocessor {
	private DicomInputStream _dataInputStream = null;
	private Attributes _dataset = null;
	private Attributes _metaData = null;
	private byte[] _rawPixelData = null;
	private short[] _convertedPixelData = null;
	private String _tsuid  = null;
	private float _rescaleSlope = Float.MIN_VALUE;
	private float _rescaleIntercept = Float.MIN_VALUE;
	private float _impagePosition2 = Float.MIN_VALUE;
	private float _sliceLocation = Float.MIN_VALUE;
	private static final Logger logger = Logger.getLogger(DicomFileDataPreprocessor.class);
	private static final String NO_PIXEL_DATA_MSG = "No Raw Pixel Data for  DICOM File";
	private static final String CONVERTED_PIXELS_MSG_PREFIX = "Coverted Pixel Data Length in Shorts: ";
	private static final String NO_CONVERT_PIXELS_BYTES_TO_SHORTS_MSG = "Could not convert pixel data in bytes to an array of shorts";
	private static final String PROBLEM_CREATING_DICOM_ATTRIBUTES_BYTE_ARRAY_MSG = "Problems creating byte array from a DICOM Attributes class.";
	private static final String DEFAULT_TRANSFER_SYNTAX_UID ="1.2.840.10008.1.2.1";

	public DicomFileDataPreprocessor(byte[] bytesAsDicomFile) throws Exception {
		super();
		ByteArrayInputStream diconFileInputStream = new ByteArrayInputStream(bytesAsDicomFile);
		_dataInputStream = new DicomInputStream(diconFileInputStream);
		initialize();
	}

	public DicomFileDataPreprocessor(File dicomFile) throws Exception {
		super();
		_dataInputStream = new DicomInputStream(dicomFile);
		initialize();
	}

	private void initialize() throws Exception {
		_metaData = _dataInputStream.getFileMetaInformation();
		logger.info("DICOM file metadata: " + _metaData);
		_tsuid = _metaData.getString(Tag.TransferSyntaxUID, DEFAULT_TRANSFER_SYNTAX_UID);
		_dataset = _dataInputStream.readDataset(-1, -1);
		logger.info("DICOM file attributes: " + _dataset);
		_rescaleSlope = _dataset.getFloat(Tag.RescaleSlope, 1);
		_rescaleIntercept = _dataset.getFloat(Tag.RescaleIntercept, 1);
		_impagePosition2 = _dataset.getFloat(Tag.ImagePositionPatient, 2, 0);
		_sliceLocation = _dataset.getFloat(Tag.SliceLocation, 0);
		_rawPixelData = Converter.getPixelData(_dataset);
		if (_rawPixelData != null) {
			logger.info("Raw Pixel Data Length in Bytes: " + _rawPixelData.length);
		} else {
			logger.error(NO_PIXEL_DATA_MSG);
			Exception ex = new Exception(NO_PIXEL_DATA_MSG);
			ex.printStackTrace();
			throw ex;
		}
		_convertedPixelData = ByteUtilities.byteArrayToShortArray(_rawPixelData);
		if (_convertedPixelData != null) {
			logger.info(CONVERTED_PIXELS_MSG_PREFIX + _convertedPixelData.length);
		} else {
			logger.error(NO_CONVERT_PIXELS_BYTES_TO_SHORTS_MSG);
			Exception ex = new Exception(NO_CONVERT_PIXELS_BYTES_TO_SHORTS_MSG);
			ex.printStackTrace();
			throw ex;
		}

	}

	public byte[] execute() throws IOException{
		for (int i = 0; i < _convertedPixelData.length; i++) {
			short hounsefieldUnit = Converter.toHounsefieldUnit(_convertedPixelData[i], _rescaleSlope,
					_rescaleIntercept);
			short normalizedHounsefieldUnit = Converter.normanlizeHounsefieldUnit(hounsefieldUnit, -1000, 400);
			short zeroCenteredHounsefieldUnit = Converter.zeroCenter(normalizedHounsefieldUnit, 0.25);
			_convertedPixelData[i] = zeroCenteredHounsefieldUnit;
		}
		
		/*
		
		logger.info("*************Printing 250 characters from Short Array !!" );
		printCharactersFromShortArray(250, _convertedPixelData);
		
		*/
		
		byte[] pixelsAsByteArray = ByteUtilities.shortArrayToByteArray(_convertedPixelData);
		logger.info("Converted Pixel Data Length in After Conversion Back to Bytes: " + pixelsAsByteArray.length);
		
		/*
		
		logger.info("**********Printing 500 characters from Byte Array !!" );
		printCharactersFromByteArray(500, pixelsAsByteArray);
		logger.info("**********Converting Byte Array characters Back to Short Array !!" );
		short[] _convertedPixelData2 = ByteUtilities.byteArrayToShortArray(pixelsAsByteArray);
		logger.info("**********Printing 250 characters from short Array !!" );
		printCharactersFromShortArray(250, _convertedPixelData2);
		
		*/
		
		
		_dataset.remove(Tag.PixelData);
		_dataset.setBytes(Tag.PixelData, VR.OW, pixelsAsByteArray);
		System.out.println("Attributes after cleanup: " + _dataset);
		byte[] bytes =  null;
		try{
				bytes = ByteUtilities.dicomAttributesToByteArray(_dataset);
		}
		catch(IOException ex){
			logger.error(PROBLEM_CREATING_DICOM_ATTRIBUTES_BYTE_ARRAY_MSG);
			ex.printStackTrace();
			throw ex;
		}
		//return bytes;
		return writeDicomOuput(_metaData, _dataset);
	}
	
 
    public void printCharactersFromByteArray(int numberOfBytesToPrint, byte[] bytes){
    	for (int i =0; i < numberOfBytesToPrint; i++){
    		logger.info("byte[" + i + "] value is " + bytes[i]);
    	}
    }
    
    public void printCharactersFromShortArray(int numberOfShortsToPrint, short[] shorts){
    	for (int i =0; i < numberOfShortsToPrint; i++){
    		logger.info("shorts[" + i + "] value is " + shorts[i]);
    	}
    }
    
    private byte[] writeDicomOuput(Attributes metaData, Attributes dataset) throws IOException{
    	ByteArrayOutputStream bais = new ByteArrayOutputStream();
    	DicomOutputStream dos = new DicomOutputStream(bais, _tsuid);
    	dos.writeDataset(metaData, dataset);
    	dos.flush();
    	dos.close();
    	return bais.toByteArray();
    }
    
}
