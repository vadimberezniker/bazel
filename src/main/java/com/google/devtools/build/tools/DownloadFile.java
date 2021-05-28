package com.google.devtools.build.tools;

import java.io.File;
import java.util.Arrays;

public class DownloadFile {
    public static void main(String[] args) throws Exception{
        if (args.length == 0) {
            throw new IllegalArgumentException("specify an digest+size");
        }

        BazelEnv env = new BazelEnv(Arrays.asList(args).subList(1, args.length));

        for (int i = 0; i < 1; i++) {
            System.out.println("Downloading file, loop " + i);
            env.downloadFile(args[0]);
        }
    }
}
