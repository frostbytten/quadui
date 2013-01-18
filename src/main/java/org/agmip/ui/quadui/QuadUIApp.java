package org.agmip.ui.quadui;

import org.apache.pivot.beans.BXMLSerializer;
import org.apache.pivot.collections.Map;
import org.apache.pivot.wtk.Application;
import org.apache.pivot.wtk.DesktopApplicationContext;
import org.apache.pivot.wtk.Display;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QuadUIApp extends Application.Adapter {
    private QuadUIWindow window = null;
    private static Logger LOG = LoggerFactory.getLogger(QuadCmdLine.class);

    @Override
    public void startup(Display display, Map<String, String> props) throws Exception {
        LOG.info("Run GUI start");
        BXMLSerializer bxml = new BXMLSerializer();
        window = (QuadUIWindow) bxml.readObject(getClass().getResource("/quadui.bxml"));
        window.open(display);
    }

    @Override
    public boolean shutdown(boolean opt) {
        if (window != null) {
            window.close();
        }
        return false;
    }

    public static void main(String[] args) {
        boolean cmdFlg = false;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("-cli")) {
                cmdFlg = true;
                break;
            }
        }
        if (cmdFlg) {
            LOG.info("Run CLI");
            QuadCmdLine cmd = new QuadCmdLine();
            cmd.run(args);
            
        } else {
            LOG.info("Run GUI");
            DesktopApplicationContext.main(QuadUIApp.class, args);
        }
    }
}
