package com.wang.file;

import com.wang.MiniDB;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.Charset;

public class FileTest {
    @Test
    public void test() throws IOException {

        MiniDB.init();
        FileManager fm = MiniDB.getFm();
        Block blk = new Block("test", 0);

        Page p1 = new Page();
        p1.read(blk);
        int n = p1.getInt(20);
        p1.setInt(20, n + 1);
        p1.write(blk);

        Page p2 = new Page();
        p2.setString(25, "hello");
        blk = p2.append("test");
        Page p3 = new Page();
        p3.read(blk);
        String s = p3.getString(25);
        System.out.println("Block: " + blk.getBlockNum() + " contains " + s);

    }
}
