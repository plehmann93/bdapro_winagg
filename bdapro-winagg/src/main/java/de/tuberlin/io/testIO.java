package de.tuberlin.io;

import org.ini4j.Wini;

import java.io.File;
import java.io.IOException;

/**
 * Created by Lehmann on 24.11.2016.
 */
public class testIO {

    void sample01(String filename) throws IOException {
        Wini ini = new Wini(new File(filename));
        int age = ini.get("happy", "age", int.class);
        double height = ini.get("happy", "height", double.class);
        String dir = ini.get("happy", "homeDir");
        System.out.println(age+" "+height+" "+dir);
    }

    public static void main(String[] args) throws Exception {
        testIO testIO=new testIO();
        testIO.sample01("src/main/resources/config.ini");
    }
}
