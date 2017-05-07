package cs498.final_project.hbase;

import cs498.final_project.dicom.DicomObject;
import cs498.final_project.dicom.DicomFilePersister;

public class RowRetriever {
	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("usage: java cs498.final_project.hbase.RowRetiever <patient id> <instance number> ");
			System.exit(-1);
		}
		String patientId = args[0];
		Integer instanceNumber = null;

		try {
			instanceNumber = Integer.valueOf(args[1]);
		} catch (Exception ex) {
			System.out.println("<instance number> must be an integer.");
			System.exit(-1);
			;
		}
		DicomFilePersister persister = new DicomFilePersister();
		DicomObject dicomObject = null;
		try {
			dicomObject = persister.reteiveByRowId(patientId, instanceNumber.intValue());
			if (dicomObject != null) {
				System.out.println(dicomObject);
			} else {
				System.out.print("No row was retrieved !");
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}
}
