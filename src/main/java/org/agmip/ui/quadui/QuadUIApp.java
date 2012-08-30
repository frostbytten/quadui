package org.agmip.ui.quadui;

import org.apache.pivot.beans.BXMLSerializer;
import org.apache.pivot.collections.Map;
import org.apache.pivot.wtk.Application;
import org.apache.pivot.wtk.DesktopApplicationContext;
import org.apache.pivot.wtk.Display;

public class QuadUIApp extends Application.Adapter {
    private QuadUIWindow window = null;

    @Override
    public void startup(Display display, Map<String, String> props) throws Exception {
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
        DesktopApplicationContext.main(QuadUIApp.class, args);
    }
}
