package com.wang.buffer;

import com.wang.MiniDB;
import com.wang.file.Block;
import com.wang.file.Page;

/**
 * 缓冲区
 */
public class Buffer {

    private Page contents = new Page();

    private Block blk = null;

    // 当前缓冲页固定的次数，有点多线程中ReentranLock的意思，一个客户端可以pin多次
    private int pins = 0;
    // 和事务相关 TODO
    private int modifiedBy = -1;  // 当前缓冲区对应哪个事务
    private int logSequenceNum = -1;  // LSN

    /**
     * 一个客户端可以读取已固定的缓冲区中的具体内容，通过的是调用getInt()和getString()方法
     * @param offset
     * @return
     */
    public int getInt(int offset) {
        return contents.getInt(offset);
    }
    public String getString(int offset) {
        return contents.getString(offset);
    }

    /**
     * 修改缓冲区的值，修改值需要记录日志
     * @param offset
     * @param val
     * @param txNum
     * @param LSN
     */
    public void setInt(int offset,int val,int txNum,int LSN) {
        // 绑定事务id
        modifiedBy = txNum;
        if(LSN>=0) {
            logSequenceNum = LSN;
        }
        contents.setInt(offset, val);
    }
    public void setString(int offset,String val,int txNum,int LSN) {
        modifiedBy = txNum;
        // LSN的当前实现就是日志文件块的块号
        if (LSN >= 0)
            logSequenceNum = LSN;
        contents.setString(offset, val);
    }
    public Block block() {
        return blk;
    }

    /**
     * 将缓冲页中的内容写回到磁盘，且在写回数据到磁盘前，追加一条日志记录。
     * 1. 先将内存页写回磁盘
     * 2. flush日志记录
     */
    void flush() {
        if (modifiedBy >= 0) {
            contents.write(blk);
            MiniDB.getLm().flush(logSequenceNum);
        }
    }

    void pin() {
        pins++;
    }

    void unpin() {
        pins--;
    }

    boolean isPinned() {
        return pins > 0;
    }

    boolean isModifyiedBy(int txNum) {
        return txNum == modifiedBy;
    }

    /**
     * 将指定的块中内容，赋值到缓冲页上。
     * 注意，在赋值前，要检查下当前页的内容是否被修改过！
     * 如果被修改过，必须先在写回磁盘前写一条日志记录，然后再执行相关操作。
     *
     * @param b 待写回的块
     */
    void assignToBlock(Block b) {
        flush();
        blk = b;
        contents.read(blk);
        pins = 0;
    }

    /**
     * 将缓冲页的内容格式化，再追加到文件块
     * 注意，在追加磁盘块前，也要检查下当前页的内容是否被修改过！
     * 如果被修改过，必须先写回磁盘并记录日志
     * 然后再执行缓冲页的格式化操作，再追加回磁盘块中。
     *
     * @param fileName
     * @param pfm
     */
    void assignToNewBlock(String fileName, PageFormatter pfm) {
        flush();
        pfm.format(contents);
        blk = contents.append(fileName);
        pins = 0;
    }
}
