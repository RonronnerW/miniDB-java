package com.wang.buffer;

import com.wang.file.Block;

/**
 * 缓冲管理器-管理缓冲池
 */
public class BasicBufferManager {

    private Buffer[] bufferPool;  // 缓冲池
    private int numAvailable;   // 空闲缓冲区数量

    /**
     * 构造函数，数据库初始化时传入缓冲池大小
     * @param numBuffers 缓冲池大小
     */
    public BasicBufferManager(int numBuffers) {
        this.numAvailable = numBuffers;
        bufferPool = new Buffer[numBuffers];
        for (int i = 0; i < numBuffers; i++) {
            bufferPool[i] = new Buffer();
        }
    }

    /**
     * 固定一个块到一个缓冲区
     *  @param blk 待固定的块
     *  @return 固定成功的缓冲区对象 或 null（表示需要等待）
     */
    public synchronized Buffer pin(Block blk) {
        Buffer buffer = findExistingBuffer(blk);
        // 1. 块中的内容不在缓存中
        if(buffer==null) {
            buffer = chooseUnpinnedBuffer();
            // 1.1 缓冲池中所有的页都被固定了
            if (null == buffer)
                return null;
            // 1.2 存在未固定的页就关联当前块
            buffer.assignToBlock(blk);
        }
        // 2. 如果存在一个缓冲区的内容就是待关联的块，此时有2种情况：
        //      2.1 该缓冲区已经被固定，即 pins > 0,有可能是当前客户端之前固定过该块，或者是其他客户端固定过该块，
        //          在这里，我们并不关心是被缓冲区是被哪个客户端pin的。
        //      2.2 如果该缓冲区没被固定,即该缓冲区上的block是新替换的，即 pins == 0
        if (!buffer.isPinned())
            numAvailable--;
        // pins++
        buffer.pin();

        return buffer;

    }

    /**
     * 取消固定缓冲区
     * @param buffer
     */
    public synchronized void unpin(Buffer buffer) {
        buffer.unpin();
        if(!buffer.isPinned()) {
            numAvailable ++;
        }
    }

    /**
     * 负责创建新的文件块
     * 会为这个新的块分配一个新的缓冲页，固定这个页，并且格式化好，随后将这个格式化好的页追加到文件后，并返回这个缓冲区给客户端
     * @param fileName
     * @param pageFormatter
     * @return
     */
    public synchronized Buffer pinNew(String fileName, PageFormatter pageFormatter) {
        Buffer buffer = chooseUnpinnedBuffer();
        if(null == buffer) {
            return null;
        }
        // 追加新磁盘块
        buffer.assignToNewBlock(fileName, pageFormatter);
        buffer.pin();
        numAvailable --;
        return buffer;
    }

    /**
     * 清理指定事务，将事务涉及的页持久化
     * @param txNum
     */
    public void  flushAll(int txNum) {
        for (Buffer buffer : bufferPool) {
            if(buffer.isModifyiedBy(txNum)) {
                buffer.flush();
            }
        }
    }

    /**
     * 可用缓冲区数量
     *
     * @return int
     */
    int available() {
        return numAvailable;
    }

    /**
     * 是否磁盘块当前正在内存页中
     *
     * @param block 待查找的块引用
     * @return 如果存在，就返回那个缓冲区；否则返回null
     */
    private Buffer findExistingBuffer(Block block) {
        for (Buffer buff : bufferPool) {
            Block blkInBuffer = buff.block();
            if (blkInBuffer != null && blkInBuffer.equals(block)) {
                return buff;
            }
        }
        return null;
    }

    /**
     * 在缓冲池中找一个没被固定的页
     * TODO: 当前用的最简单的Naive算法，找到一个就OK,后期考虑其他策略。
     *
     * @return 如果存在，就返回那个缓冲区；否则返回null
     */
    private Buffer chooseUnpinnedBuffer() {
        for (Buffer buff : bufferPool) {
            if (!buff.isPinned()) {
                return buff;
            }
        }
        return null;
    }
}
