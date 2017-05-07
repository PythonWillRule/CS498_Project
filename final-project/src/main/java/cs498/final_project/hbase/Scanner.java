package cs498.final_project.hbase;

import cs498.final_project.dicom.DicomObject;
import cs498.final_project.dicom.DicomFilePersister;
import java.util.ArrayList;

public class Scanner {
	public static void main(String[] args) {
		if (args.length != 4) {
			System.out.println("usage: java cs498.final_project.hbase.Scanner < starting patient id> <starting instance number> <ending patient id> <ending instance number>");
			System.exit(-1);
		}
		String startingPatientId = args[0];
		String endingPatientId = args[2];
		Integer startingInstanceNumber = null;
		Integer endingInstanceNumber = null;
		ArrayList<DicomObject> dicomObjects = null;

		try {
			startingInstanceNumber = Integer.valueOf(args[1]);
			endingInstanceNumber = Integer.valueOf(args[3]);
			
		} catch (Exception ex) {
			System.out.println("<instance number> must be an integer.");
			System.exit(-1);
			;
		}
		DicomFilePersister persister = new DicomFilePersister();
		
		try {
			dicomObjects = persister.scan(startingPatientId, startingInstanceNumber, endingPatientId, endingInstanceNumber);
			if (dicomObjects != null && !dicomObjects.isEmpty()) {
				 for (DicomObject dicomObject : dicomObjects){
			        	System.out.println("Dicom Object: " + dicomObject);
			        }
			} else {
				System.out.print("No row was retrieved !");
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}
}
