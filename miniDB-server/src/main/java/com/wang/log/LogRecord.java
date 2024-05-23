package com.wang.log;

import com.wang.file.Page;

import static com.wang.file.Page.INT_SIZE;
import static com.wang.file.Page.STR_SIZE;

/**
 * 构造函数接受一个日志页中指示一条日志记录的起始位置，会使用nextInt()和nextString()方法来读取后续的数据，然后在读的过程中会移动位置指针。
 */
public class LogRecord {

    private Page logPage;
    private int pos;


    public LogRecord(Page logPage, int pos) {
        this.logPage = logPage;
        this.pos = pos;
    }

    public int nextInt() {
        int intVal = logPage.getInt(pos);
        pos += INT_SIZE;
        return intVal;
    }

    public String nextString() {
        String stringVal = logPage.getString(pos);
        pos += STR_SIZE(stringVal.length());
        return stringVal;
    }
}