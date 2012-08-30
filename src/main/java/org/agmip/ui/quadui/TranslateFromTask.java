package org.agmip.ui.quadui;

import java.util.LinkedHashMap;
import org.apache.pivot.util.concurrent.Task;
import org.agmip.core.types.TranslatorInput;
import org.agmip.translators.csv.CSVInput;

public class TranslateFromTask extends Task<LinkedHashMap> {
	private String file;
	private TranslatorInput translator;
	
	public TranslateFromTask(String file) {
		this.file = file;
		translator = new CSVInput();
	}
	
	@Override
	public LinkedHashMap<String, Object> execute() {
		LinkedHashMap<String, Object> output = new LinkedHashMap<String, Object>();
		try {
			output = (LinkedHashMap<String, Object>) translator.readFile(file);
			return output;
		} catch (Exception ex) {
			output.put("errors", ex.getMessage());
			return output;
		}
	}
}
