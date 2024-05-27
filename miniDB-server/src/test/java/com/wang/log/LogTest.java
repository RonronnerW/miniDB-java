package com.wang.log;

import com.wang.MiniDB;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

public class LogTest {
    @Test
    public void test() {
        MiniDB miniDB = new MiniDB();
        miniDB.init();
        LogManager logMgr = miniDB.getLm();
        int lsn1 = logMgr.append(new Object[]{"a", "b"});
        int lsn2 = logMgr.append(new Object[]{"c", "d"});
        int lsn3 = logMgr.append(new Object[]{"e", "f"});
        logMgr.flush(lsn3);

        Iterator<BasicLogRecord> iter = logMgr.iterator();
        while (iter.hasNext()) {
            BasicLogRecord rec = iter.next();
            String v1 = rec.nextString();
            String v2 = rec.nextString();
            System.out.println("[" + v1 + ", " + v2 + "]");
        }
    }
}
