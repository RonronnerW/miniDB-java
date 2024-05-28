package com.wang.tx;

import com.wang.MiniDB;
import com.wang.buffer.Buffer;
import com.wang.buffer.BufferManager;
import com.wang.buffer.PageFormatter;
import com.wang.file.Block;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 事务当前固定的缓冲区
 */
public class BufferList {
    /**
     * 事务相关的所有缓冲区
     */
    private Map<Block, Buffer> buffers = new HashMap<>();
    /**
     * 缓冲区固定次数
     * 固定了块b两次，那么list中就包含两个块b对象
     */
    private List<Block> pins = new ArrayList<>();
    private BufferManager bufferMgr = MiniDB.getBm();

    Buffer getBuffer(Block blk) {
        return buffers.get(blk);
    }

    void pin(Block blk) throws InterruptedException {
        Buffer buff = bufferMgr.pin(blk);
        buffers.put(blk, buff);
        pins.add(blk);
    }

    Block pinNew(String fileName, PageFormatter pfmt) throws InterruptedException {
        Buffer buff = bufferMgr.pinNew(fileName, pfmt);
        Block blk = buff.block();
        buffers.put(blk, buff);
        pins.add(blk);
        return blk;
    }

    void unpin(Block blk) {
        Buffer buff = buffers.get(blk);
        bufferMgr.unpin(buff);
        pins.remove(blk);

        if (!pins.contains(blk))
            buffers.remove(blk);
    }

    void unpinAll() {
        for (Block blk : pins) {
            Buffer buff = buffers.get(blk);
            bufferMgr.unpin(buff);
        }

        buffers.clear();
        pins.clear();
    }
}
