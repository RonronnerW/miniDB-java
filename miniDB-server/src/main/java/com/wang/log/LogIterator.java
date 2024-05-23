package com.wang.log;

import com.wang.file.Block;
import com.wang.file.Page;

import java.util.Iterator;

import static com.wang.file.Page.INT_SIZE;
import static com.wang.log.LogManager.LAST_POS;

/**
 * 构造器会将迭代器的初始位置指向日志文件最后一块的最后一条记录。next()方法使用相应的指针来往之前的日志记录不断迭代
 */
public class LogIterator implements Iterator<LogRecord> {

    private Block blk;
    private Page page = new Page();
    private int currentRec;  // 迭代器当前遍历的日志记录结束位置

    public LogIterator(Block blk) {
        this.blk = blk;
        page.read(blk);
        // 初始化为最后一条日志记录的结束位置
        currentRec = page.getInt(LAST_POS);
    }

    @Override
    public boolean hasNext() {
        return currentRec > 0 || blk.getBlockNum() > 0;
    }

    @Override
    public LogRecord next() {
        // 迭代上一块
        if (0 == currentRec)
            moveToNextBlock();
        // 继续往回迭代上一条
        currentRec = page.getInt(currentRec);
        return new LogRecord(page, currentRec + INT_SIZE);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Remove operation is not supported in LogIterator！");
    }

    private void moveToNextBlock() {
        // 上一个块
        blk = new Block(blk.getFileName(), blk.getBlockNum() - 1);
        page.read(blk);
        currentRec = page.getInt(LAST_POS);
    }
}