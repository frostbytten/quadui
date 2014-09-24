package org.agmip.ui.quadui;

import java.util.ArrayList;
import java.util.HashMap;
import org.agmip.dome.Engine;
import org.agmip.util.MapUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplyDomeRunner implements Runnable {

    private ArrayList<Engine> engines;
    private HashMap entry;
    private boolean noExpMode;
    private String mode;
    private static Logger LOG = LoggerFactory.getLogger(ApplyDomeRunner.class);

    public ApplyDomeRunner(ArrayList<Engine> engines, HashMap entry, boolean noExpMode, String mode) {
        this.engines = engines;
        this.entry = entry;
        this.noExpMode = noExpMode;
        this.mode = mode;
    }

    @Override
    public void run() {
        for (Engine engine : engines) {
            LOG.debug("Starting new thread!");
            LOG.info("Apply DOME {} for {}", engine.getDomeName(), MapUtil.getValueOr(entry, "exname", MapUtil.getValueOr(entry, "soil_id", MapUtil.getValueOr(entry, "wst_id", "<Unknow>"))));
            engine.apply(flatSoilAndWthData(entry, noExpMode));
            ArrayList<String> strategyList = engine.getGenerators();
            if (!strategyList.isEmpty()) {
                LOG.warn("The following DOME commands in the field overlay file are ignored : {}", strategyList.toString());
            }
            if (!noExpMode && !mode.equals("strategy")) {
                // Check if there is no weather or soil data matched with experiment
                if (((HashMap) MapUtil.getObjectOr(entry, "weather", new HashMap())).isEmpty()) {
                    LOG.warn("No baseline weather data found for: [{}]", MapUtil.getValueOr(entry, "exname", "N/A"));
                }
                if (((HashMap) MapUtil.getObjectOr(entry, "soil", new HashMap())).isEmpty()) {
                    LOG.warn("No soil data found for:    [{}]", MapUtil.getValueOr(entry, "exname", "N/A"));
                }
            }
        }
    }

    private HashMap<String, Object> flatSoilAndWthData(HashMap<String, Object> data, boolean noExpFlg) {
        if (!noExpFlg) {
            return data;
        }
        HashMap<String, Object> ret;
        if (data.containsKey("dailyWeather")) {
            ret = new HashMap<String, Object>();
            ret.put("weather", data);
        } else if (data.containsKey("soilLayer")) {
            ret = new HashMap<String, Object>();
            ret.put("soil", data);
        } else {
            ret = data;
        }
        return ret;
    }
}
