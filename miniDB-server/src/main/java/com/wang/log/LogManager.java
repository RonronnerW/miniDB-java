package com.wang.log;

import com.wang.MiniDB;
import com.wang.file.Block;
import com.wang.file.Page;

import java.io.IOException;
import java.util.Iterator;

import static com.wang.file.Page.*;

/**
 * 日志管理器
 */
public class LogManager {

    /**
     * 标识最后一条日志记录的结束位置的指针，它本身也是在页中的内容
     * 即[LAST_POS...LAST_POS+3]这4个字节代表的整数标识了最后一条日志记录的结束位置
     */
    public static final int LAST_POS = 0;

    /**
     *
     */
    private String logFile;

    /**
     * 日志页
     */
    private Page logPage = new Page();

    /**
     * 当前磁盘块
     */
    private Block currentBlk;

    /**
     * 当前页偏移
     */
    private int currentPos;

    /**
     * 传入日志文件名，文件为空则追加新的空日志块，否则初始化为磁盘块的最后一块内容
     * @param logfile 日志文件名
     * @throws IOException
     */
    public LogManager(String logfile) throws IOException {
        this.logFile = logfile;
        int logSize = MiniDB.getFm().size(logfile);
        // 如果日志文件为空，则为日志文件追加一个新的空日志块
        if (0 == logSize)
            appendNewBlock();
        else {
            // 否则先读入最后一块.
            // 注意，块号的下标从0开始，所以要减去1
            currentBlk = new Block(logfile, logSize - 1);
            logPage.read(currentBlk);
            // 最后一条日志记录的结束位置
            currentPos = getLastRecordPosition() + INT_SIZE;
        }

    }

    /**
     * 添加日志记录到日志文件，返回日志序列号
     * 追加一条日志记录后并不会保证这条记录会立马被写入到磁盘中
     * @param rec
     * @return
     */
    public synchronized int append(Object[] rec) {
        int recSize = INT_SIZE; // 一条记录的字节数
        for (Object obj : rec) {
            recSize += size(obj);
        }
        // 如果追加一条日志记录后，当前页放不下-> 写入磁盘并增加一个块
        if (currentPos + recSize >= BLOCK_SIZE) {
            flush();
            appendNewBlock();
        }
        // 把当前这条日志记录中的值全部依次放入日志页中
        for (Object o : rec) {
            appendVal(o);
        }
        finalizeRecord();

        return currentLSN();
    }

    /**
     * 将该日志序列号及之前的日志写入磁盘
     * @param lsn
     */
    public void flush(int lsn) {
        // 如果lsn小于当前的块号，则说明对应的日志已经被写回磁盘了
        // 否则执行一次写磁盘操作
        if (lsn >= currentLSN())
            flush();
    }

    /**
     * 迭代器 读取日志文件的所有日志记录
     * iterator()方法返回的所有日志记录刚好是一个反的顺序，从最近的日志记录开始，不断往前移动
     * @return
     */
    public Iterator<BasicLogRecord> iterator() {
        flush(); // 先持久化日志，确保日志都在磁盘上
        return new LogIterator(currentBlk);
    }

    /**
     * 返回当前LSN 即返回当前块的块号作为LSN。
     *
     * @return
     */
    private int currentLSN() {
        return currentBlk.getBlockNum();
    }

    /**
     * 处理追加完日志记录后的动作：
     *      1. 先在当前日志记录后面加上一个整数，用来标识上一条日志记录的结束位置。
     *      2. 再改变日志页的最开始的4个字节，用来直接标识最后一条日志记录的结束位置。
     * 这一部分类似一个逆着的数组链表，务必理清其中的逻辑
     */
    private void finalizeRecord() {
        logPage.setInt(currentPos, getLastRecordPosition());
        setLastRecordPosition(currentPos);

        currentPos += INT_SIZE;
    }

    /**
     * 将日志记录中的值追加到日志页logPage中
     * TODO: 目前该数据库系统只支持int和string类型，以后扩展后次方法也要对应扩展
     *
     * @param obj 追加的值
     */
    private void appendVal(Object obj) {
        if (obj instanceof String) {
            logPage.setString(currentPos, (String) obj);
        } else {
            logPage.setInt(currentPos, (Integer) obj);
        }
        currentPos += size(obj);
    }

    /**
     * 将当前页中的内容持久化到磁盘中
     */
    private void flush() {
        logPage.write(currentBlk);
    }

    /**
     * 计算一个obj的字节需要使用的字节数
     * TODO: 目前该数据库系统只支持int和string类型，以后扩展后次方法也要对应扩展
     *
     * @param obj 待统计的对象
     * @return 字节数
     */
    private int size(Object obj) {
        if (obj instanceof String) {
            String strVal = (String) obj;
            return STR_SIZE(strVal.length());
        } else {
            return INT_SIZE;
        }
    }

    /**
     * 追加一个新的空块到日志文件
     */
    private void appendNewBlock() {
        // 设置最后一条日志记录的结束位置为0
        setLastRecordPosition(0);
        currentBlk = logPage.append(logFile);
        // 新分配的日志块肯定只有一个INT，也就是4个字节
        // 该INT表示的是最后一条日志记录的结束位置，在这里为0
        currentPos = INT_SIZE;
    }

    private void setLastRecordPosition(int pos) {
        // 第一个参数为offset，第二个参数为具体的值
        logPage.setInt(LAST_POS, pos);
    }

    private int getLastRecordPosition() {
        return logPage.getInt(LAST_POS);
    }

}
