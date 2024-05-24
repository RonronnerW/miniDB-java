package com.wang.buffer;

import com.wang.file.Block;

/**
 * 管理等待队列
 * 对缓冲管理器的包装-没有空闲缓冲区就加入等待队列
 */
public class BufferManager {
    private BasicBufferManager basicBufferManager;

    public BufferManager(BasicBufferManager basicBufferManager) {
        this.basicBufferManager = basicBufferManager;
    }

    public synchronized Buffer pin(Block blk) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        Buffer buffer = basicBufferManager.pin(blk);
        while(null == buffer && !waitTooLong(startTime)) {
            // wait()，等待的对象是当前这个缓冲管理器，
            // 等待的目标是有一个未被固定的缓冲区
            wait(BufferConstant.TIME_OUT);
            buffer = basicBufferManager.pin(blk);
        }
        if(null == buffer) {
            throw new BufferAbortException();
        }
        return buffer;
    }


    public synchronized void unpin(Buffer buffer) {
        basicBufferManager.unpin(buffer);
        // 有未固定缓冲区则通知其他线程
        if(!buffer.isPinned()) {
            notifyAll();
        }
    }


    public synchronized Buffer pinNew(String fileName, PageFormatter pageFormatter) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        Buffer buffer = basicBufferManager.pinNew(fileName, pageFormatter);
        while(null == buffer && !waitTooLong(startTime)) {
            // wait()，等待的对象是当前这个缓冲管理器，
            // 等待的目标是有一个未被固定的缓冲区
            wait(BufferConstant.TIME_OUT);
            buffer = basicBufferManager.pinNew(fileName, pageFormatter);
        }
        if(null == buffer) {
            throw new BufferAbortException();
        }
        return buffer;
    }

    public void flushAll(int txNum) {
        basicBufferManager.flushAll(txNum);
    }

    public int available() {
        return basicBufferManager.available();
    }


    private boolean waitTooLong(long startTime) {
        return System.currentTimeMillis() - startTime > BufferConstant.TIME_OUT;
    }
}
