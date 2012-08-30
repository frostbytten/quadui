package org.agmip.ui.quadui;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.ArrayList;
import java.util.LinkedHashMap;


import org.apache.pivot.util.concurrent.Task;
import org.apache.pivot.util.concurrent.TaskExecutionException;

import org.agmip.core.types.TranslatorOutput;
import org.agmip.translators.apsim.ApsimOutput;
import org.agmip.translators.dssat.DssatControllerOutput;

public class TranslateToTask extends Task<String> {
	private LinkedHashMap data;
	private ArrayList<String> translateList;
	private ArrayList<String> weatherList, soilList;
	private String destDirectory;


	public TranslateToTask(ArrayList<String> translateList, LinkedHashMap data, String destDirectory) {
		this.data = data;
		this.destDirectory = destDirectory;
		this.translateList = new ArrayList<String>();
		this.weatherList = new ArrayList<String>();
		this.soilList = new ArrayList<String>();
		for(String trType : translateList) {
			if(! trType.equals("JSON")) {
				this.translateList.add(trType);
			}
		}
		if (data.containsKey("weathers")) {
			for (LinkedHashMap<String, Object> stations : (ArrayList<LinkedHashMap>) data.get("weathers")) {
				weatherList.add((String) stations.get("wst_id"));
			}
		}

		if (data.containsKey("soils")) {
			for (LinkedHashMap<String, Object> soils : (ArrayList<LinkedHashMap>) data.get("soils")) {
				soilList.add((String) soils.get("soil_id"));
			}
		}
	}

	@Override
	public String execute() throws TaskExecutionException {
		ExecutorService executor = Executors.newFixedThreadPool(64);
		try {
			for( String tr : translateList ) {
				if (data.containsKey("experiments")) {
					for (LinkedHashMap<String, Object> experiment : (ArrayList<LinkedHashMap>) data.get("experiments")) {
						//LinkedHashMap<String, Object> experiment = ((ArrayList<LinkedHashMap<String,Object>>)data.get("experiments")).get(0);
						LinkedHashMap<String, Object> temp = new LinkedHashMap<String, Object>(experiment);
						int wKey, sKey;
						if (temp.containsKey("wst_id")) {
							if ((wKey = weatherList.indexOf((String) temp.get("wst_id"))) != -1) {
								temp.put("weather", ((ArrayList<LinkedHashMap<String, Object>>) data.get("weathers")).get(wKey));
							}
						}
						if (temp.containsKey("soil_id")) {
							if ((sKey = soilList.indexOf((String) temp.get("soil_id"))) != -1) {
								temp.put("soil", ((ArrayList<LinkedHashMap<String, Object>>) data.get("soils")).get(sKey));
							}
						}

						if( tr.equals("APSIM")) {
							ApsimOutput translator = new ApsimOutput();
							String destination = destDirectory+"/APSIM";
							Runnable thread = new TranslateRunner(translator, temp, destination);
							executor.execute(thread);
						}
						if( tr.equals("DSSAT")) {
							DssatControllerOutput translator = new DssatControllerOutput();
							String destination = destDirectory+"/DSSAT";
							Runnable thread = new TranslateRunner(translator, temp, destination);
							executor.execute(thread);
						}
					}
				}
			}
			executor.shutdown();
			while(! executor.isTerminated()){}
		} catch(Exception ex) {
			ex.printStackTrace();
			throw new TaskExecutionException(ex);
		}
		return null;
	}
}
