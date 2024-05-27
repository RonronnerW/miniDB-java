package com.wang.buffer;

import com.wang.MiniDB;
import com.wang.file.Block;
import com.wang.log.LogManager;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

public class BufferTest {
    @Test
    public void test() {
        MiniDB.init();
        BasicBufferManager bufferMgr = MiniDB.getBm();
        Block blk = new Block("test", 1);
        Buffer buffer = bufferMgr.pin(blk);
        int n = buffer.getInt(20);
        String str = buffer.getString(25);
        // 客户端代码需要对unpin负责
        bufferMgr.unpin(buffer);
        System.out.println("Values are: " + n + ", " + str);


        LogManager logMgr = MiniDB.getLm();
        int myTxNum = 1; // 假设这里有个事务标识符1
        Object[] logRec = new Object[]{"test", 1, 20, n};
        int LSN=logMgr.append(logRec);

        buffer.setInt(20,n+1,myTxNum,LSN);
        // 客户端代码需要对unpin负责
        bufferMgr.unpin(buffer);
    }
}
