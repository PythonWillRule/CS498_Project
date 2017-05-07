package cs498.final_project.dicom;


import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import cs498.final_project.utils.ByteUtilities;
import java.io.IOException;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.util.ArrayList;
import java.util.Date;

public class DicomFilePersister {
	private static final Logger logger = Logger.getLogger(DicomFilePersister.class);
	private byte[] _dicomObject = null;
	private String _patientId = null;
	private Integer _instanceNumber = null;
	private String _rowIdentifier = null;
	private Configuration _config = null;
	private String _patientName = null; 
	private Date _patientBirthDate = null;
	private Date _seriesDate = null;
	private Integer _seriesNumber = null;
	private HTable _hTable = null;
	private static final byte[] TABLE_NAME = Bytes.toBytes("patient");
	private static final byte[] TABLE_COLUMNFAMILY = Bytes.toBytes("dicomfile");
	private static final byte[] COLUMN_DICOM_FILE_AS_BYTES = Bytes.toBytes("dicomFileAsBytes");
	private static final byte[] COLUMN_PATIENTID = Bytes.toBytes("patientId");
	private static final byte[] COLUMN_INSTANCE_NUMBER = Bytes.toBytes("instanceNumber");
	private static final byte[] COLUMN_PATIENT_NAME = Bytes.toBytes("patientName");
	private static final byte[] COLUMN_BIRTH_DATE = Bytes.toBytes("birthDate");
	private static final byte[] COLUMN_SERIES_DATE = Bytes.toBytes("seriesDate");
	private static final byte[] COLUMN_SERIES_NUMBER = Bytes.toBytes("seriesNumber");
	private static final String  UNSUCCESSFUL_DICOM_FILE_INSERT = "Unsuccessful Hbase Insert of Dicom file instance   ";
	private static final String  UNSUCCESSFUL_DICOM_FILE_READ = "Unsuccessful Hbase GET of Dicom file row id   ";
	

	
	public DicomFilePersister(byte[] dicomObject) throws Exception {
		super();
		_dicomObject = dicomObject;
		initialize();
		
	}
	
	public DicomFilePersister(){
		super();
	}
	
	private void initialize() throws Exception{
		 Attributes dataset = ByteUtilities.byteArrayToDicomAttributes(_dicomObject); 
		_patientId = dataset.getString(Tag.PatientID);
		_instanceNumber = dataset.getInt(Tag.InstanceNumber, 9999999);
		_rowIdentifier =  _patientId+_instanceNumber;
		_patientName = dataset.getString(Tag.PatientName);
		_patientBirthDate = dataset.getDate(Tag.PatientBirthDate);
		if (_patientBirthDate == null){
			_patientBirthDate = new Date();
		}
	    _seriesDate = dataset.getDate(Tag.SeriesDate);
	    if (_seriesDate == null){
	    	_seriesDate = new Date();
	    }
	    _seriesNumber = dataset.getInt(Tag.SeriesNumber, 9999999);
		
		
	}
	
	public boolean insert() throws Exception{
		byte [] rowid = Bytes.toBytes(_rowIdentifier);
		_config = HBaseConfiguration.create();
		_hTable = new HTable(_config, TABLE_NAME);
		Put put = new Put(rowid);
		put.add(TABLE_COLUMNFAMILY, COLUMN_DICOM_FILE_AS_BYTES, _dicomObject);
		put.add(TABLE_COLUMNFAMILY, COLUMN_PATIENTID, Bytes.toBytes(_patientId));
		put.add(TABLE_COLUMNFAMILY, COLUMN_INSTANCE_NUMBER, Bytes.toBytes(_instanceNumber));
		put.add(TABLE_COLUMNFAMILY,COLUMN_PATIENT_NAME,Bytes.toBytes(_patientName));
		put.add(TABLE_COLUMNFAMILY,COLUMN_BIRTH_DATE, Bytes.toBytes(_patientBirthDate.toString()));
		put.add(TABLE_COLUMNFAMILY,COLUMN_SERIES_DATE, Bytes.toBytes(_seriesDate.toString()));
		put.add(TABLE_COLUMNFAMILY,COLUMN_SERIES_NUMBER, Bytes.toBytes(_seriesNumber));
		boolean wasInserted = false;
		try{
			_hTable.put(put);
			wasInserted = true;
			logger.info("Successful Hbase Insert of DICOM file instance: " + _instanceNumber + " for patient: " + _patientId + " with row id: " + _rowIdentifier + ". \n");
		}
		catch(IOException ioex){
			logger.error(UNSUCCESSFUL_DICOM_FILE_INSERT + _instanceNumber + " for patiient: " + _patientId + ". \n");
			Exception ex = new Exception(UNSUCCESSFUL_DICOM_FILE_INSERT + _instanceNumber + " for patiient: " + _patientId + ". \n");
			ex.printStackTrace();
			throw ex;
		}
		finally{
			_hTable.close();
		}
		return wasInserted;
		
	}
	
	public DicomObject reteiveByRowId(String patientId, int instanceNbr) throws IOException{
		//byte[] dicomObject = null;
		String rowId = patientId+instanceNbr;
		logger.info("DicomPersister.retreiveByRowId invoked for row id: " + rowId);
		_config = HBaseConfiguration.create();
		_hTable = new HTable(_config, TABLE_NAME);
		DicomObject dicomObject = null;
		Get get = new Get(Bytes.toBytes(rowId));
		get.addColumn(TABLE_COLUMNFAMILY, COLUMN_PATIENTID);
		get.addColumn(TABLE_COLUMNFAMILY, COLUMN_INSTANCE_NUMBER);
		get.addColumn(TABLE_COLUMNFAMILY, COLUMN_PATIENT_NAME);
		get.addColumn(TABLE_COLUMNFAMILY, COLUMN_BIRTH_DATE);
		get.addColumn(TABLE_COLUMNFAMILY, COLUMN_SERIES_DATE);
		get.addColumn(TABLE_COLUMNFAMILY, COLUMN_SERIES_NUMBER);
		get.addColumn(TABLE_COLUMNFAMILY, COLUMN_DICOM_FILE_AS_BYTES);
		
		try{
			Result result = _hTable.get(get);
			//dicomObject= r.getValue(TABLE_COLUMNFAMILY, COLUMN_DICOM_FILE_AS_BYTES);
			
			dicomObject = new DicomObject();
			byte[] patientIdAsBytes = result.getValue(TABLE_COLUMNFAMILY, COLUMN_PATIENTID);
			String patient = (patientIdAsBytes != null) ? Bytes.toString(patientIdAsBytes) : "MISSING";
			dicomObject.setPatientId(patient);
			byte[] instanceNumberAsByteArray = result.getValue(TABLE_COLUMNFAMILY, COLUMN_INSTANCE_NUMBER);
			Integer instanceNumber = (instanceNumberAsByteArray != null) ? new Integer(Bytes.toInt(instanceNumberAsByteArray)) : new Integer(999999);
			dicomObject.setInstanceNumber(instanceNumber);
			byte[] patientNameAsByteArray = result.getValue(TABLE_COLUMNFAMILY, COLUMN_PATIENT_NAME);
			String patientName = (patientNameAsByteArray != null) ? Bytes.toString(patientNameAsByteArray) : "MISSING";
			dicomObject.setPatientName(patientName);
			byte[] birthDateAsByteArray = result.getValue(TABLE_COLUMNFAMILY, COLUMN_BIRTH_DATE);
			String birthDate = (birthDateAsByteArray != null) ? Bytes.toString(birthDateAsByteArray) : "MISSING";
			dicomObject.setPatientBirthDate(birthDate);
			byte[] seriesDateAsByteArray = result.getValue(TABLE_COLUMNFAMILY, COLUMN_SERIES_DATE);
			String seriesDate = (seriesDateAsByteArray != null) ? Bytes.toString(seriesDateAsByteArray) : "MISSING";
			dicomObject.setSeriesDate(seriesDate);
			byte[] seriesNumberAsByteArray = result.getValue(TABLE_COLUMNFAMILY, COLUMN_SERIES_NUMBER);
			Integer seriesNumber = (seriesNumberAsByteArray != null) ? new Integer(Bytes.toInt(seriesNumberAsByteArray)) : new Integer(999999);
			dicomObject.setSeriesNumber(seriesNumber);
			byte[] dicomObjectAsBytes = result.getValue(TABLE_COLUMNFAMILY, COLUMN_DICOM_FILE_AS_BYTES);
			dicomObject.setDicomObject(dicomObjectAsBytes);
			
		}
		catch(IOException ioex){
			logger.error(UNSUCCESSFUL_DICOM_FILE_READ + _rowIdentifier + ". \n");
		}
		finally{
			_hTable.close();
		}
		return dicomObject;
	}
	
	public  ArrayList<DicomObject> scan(String startPatientId, int startInstanceNumber, String endPatientId, int endInstanceNumber) throws IOException {
		_config = HBaseConfiguration.create();
		_hTable = new HTable(_config, TABLE_NAME);
		ArrayList<DicomObject> dicomObjects = new ArrayList<DicomObject>();
		// Instantiating Configuration class
		Configuration config = HBaseConfiguration.create();
		// Instantiating HTable class
		HTable table = new HTable(config, TABLE_NAME);
		// Instantiating the Scan class
		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes(startPatientId+startInstanceNumber));
		scan.setStartRow(Bytes.toBytes(endPatientId+(endInstanceNumber+1)));
		// Scanning the required columns
		scan.addColumn(TABLE_COLUMNFAMILY, COLUMN_PATIENTID);
		scan.addColumn(TABLE_COLUMNFAMILY, COLUMN_INSTANCE_NUMBER);
		scan.addColumn(TABLE_COLUMNFAMILY, COLUMN_PATIENT_NAME);
		scan.addColumn(TABLE_COLUMNFAMILY, COLUMN_BIRTH_DATE);
		scan.addColumn(TABLE_COLUMNFAMILY, COLUMN_SERIES_DATE);
		scan.addColumn(TABLE_COLUMNFAMILY, COLUMN_SERIES_NUMBER);
		scan.addColumn(TABLE_COLUMNFAMILY, COLUMN_DICOM_FILE_AS_BYTES);
		// Getting the scan result
		ResultScanner scanner = table.getScanner(scan);
		// Reading values from scan result
		try {
			for (Result result = scanner.next(); result != null; result = scanner.next()) {
				logger.info("Found row : " + result); 
				DicomObject dicomObject = new DicomObject();
				byte[] patientIdAsBytes = result.getValue(TABLE_COLUMNFAMILY, COLUMN_PATIENTID);
				String patient = (patientIdAsBytes != null) ? Bytes.toString(patientIdAsBytes) : "MISSING";
				dicomObject.setPatientId(patient);
				byte[] instanceNumberAsByteArray = result.getValue(TABLE_COLUMNFAMILY, COLUMN_INSTANCE_NUMBER);
				Integer instanceNumber = (instanceNumberAsByteArray != null) ? new Integer(Bytes.toInt(instanceNumberAsByteArray)) : new Integer(999999);
				dicomObject.setInstanceNumber(instanceNumber);
				byte[] patientNameAsByteArray = result.getValue(TABLE_COLUMNFAMILY, COLUMN_PATIENT_NAME);
				String patientName = (patientNameAsByteArray != null) ? Bytes.toString(patientNameAsByteArray) : "MISSING";
				dicomObject.setPatientName(patientName);
				byte[] birthDateAsByteArray = result.getValue(TABLE_COLUMNFAMILY, COLUMN_BIRTH_DATE);
				String birthDate = (birthDateAsByteArray != null) ? Bytes.toString(birthDateAsByteArray) : "MISSING";
				dicomObject.setPatientBirthDate(birthDate);
				byte[] seriesDateAsByteArray = result.getValue(TABLE_COLUMNFAMILY, COLUMN_SERIES_DATE);
				String seriesDate = (seriesDateAsByteArray != null) ? Bytes.toString(seriesDateAsByteArray) : "MISSING";
				dicomObject.setSeriesDate(seriesDate);
				byte[] seriesNumberAsByteArray = result.getValue(TABLE_COLUMNFAMILY, COLUMN_SERIES_NUMBER);
				Integer seriesNumber = (seriesNumberAsByteArray != null) ? new Integer(Bytes.toInt(seriesNumberAsByteArray)) : new Integer(999999);
				dicomObject.setSeriesNumber(seriesNumber);
				byte[] dicomObjectAsBytes = result.getValue(TABLE_COLUMNFAMILY, COLUMN_DICOM_FILE_AS_BYTES);
				dicomObject.setDicomObject(dicomObjectAsBytes);
				dicomObjects.add(dicomObject);
				
			}
		} finally {
			scanner.close();
		}
		return dicomObjects;
	}
	 
	
}
