package edu.whu.tmdb;

import android.app.Service;
import android.content.Intent;
import android.os.IBinder;

public class SqlExecService extends Service {

    private final Object sqlLock = new Object();

    private final ISqlExecService.Stub binder = new ISqlExecService.Stub() {
        @Override
        public String executeSql(String sql) {
            synchronized (sqlLock) {
                return Main.executeClientCommand(sql);
            }
        }

        @Override
        public String[] executeBatch(String[] sqlList) {
            if (sqlList == null || sqlList.length == 0) {
                return new String[0];
            }

            String[] results = new String[sqlList.length];
            synchronized (sqlLock) {
                for (int i = 0; i < sqlList.length; i++) {
                    results[i] = Main.executeClientCommand(sqlList[i]);
                }
            }
            return results;
        }
    };

    @Override
    public IBinder onBind(Intent intent) {
        return binder;
    }
}
