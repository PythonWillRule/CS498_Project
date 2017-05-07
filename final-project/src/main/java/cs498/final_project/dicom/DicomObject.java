package cs498.final_project.dicom;


public class DicomObject {
	private byte[] _dicomObject = null;
	private String _patientId = null;
	private Integer _instanceNumber = null;
	private String _patientName = null; 
	private String _patientBirthDate = null;
	private String _seriesDate = null;
	private Integer _seriesNumber = null;
	
	public byte[] getDicomObject() {
		return _dicomObject;
	}
	public void setDicomObject(byte[] _dicomObject) {
		this._dicomObject = _dicomObject;
	}
	public String getPatientId() {
		return _patientId;
	}
	public void setPatientId(String _patientId) {
		this._patientId = _patientId;
	}
	public Integer getInstanceNumber() {
		return _instanceNumber;
	}
	public void setInstanceNumber(Integer _instanceNumber) {
		this._instanceNumber = _instanceNumber;
	}
	public String getPatientName() {
		return _patientName;
	}
	public void setPatientName(String _patientName) {
		this._patientName = _patientName;
	}
	public String getPatientBirthDate() {
		return _patientBirthDate;
	}
	public void setPatientBirthDate(String _patientBirthDate) {
		this._patientBirthDate = _patientBirthDate;
	}
	public String getSeriesDate() {
		return _seriesDate;
	}
	public void setSeriesDate(String _seriesDate) {
		this._seriesDate = _seriesDate;
	}
	public Integer getSeriesNumber() {
		return _seriesNumber;
	}
	public void setSeriesNumber(Integer _seriesNumber) {
		this._seriesNumber = _seriesNumber;
	}
	
	public String toString(){
		int length = (_dicomObject != null) ? _dicomObject.length : -999999;
		StringBuilder sb = new StringBuilder();
		sb.append(" Patient Id: " + _patientId);
		sb.append(" Instance Number: " + _instanceNumber);
		sb.append(" Patient Name: " + _patientName);
		sb.append(" Patient Birth Date: " + _patientBirthDate);
		sb.append(" Series Date: " + _seriesDate);
		sb.append(" Series Number: " + _seriesNumber);
		sb.append(" Dicom Object Size in Bytes: " + length + "\n");
		return sb.toString();
	}
	
}
