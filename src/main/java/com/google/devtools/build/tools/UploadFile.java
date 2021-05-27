package com.google.devtools.build.tools;

import java.io.File;
import java.util.Arrays;

public class UploadFile {
    public static void main(String[] args) throws Exception{
        if (args.length == 0) {
            throw new IllegalArgumentException("specify an input file");
        }
        File file = new File(args[0]);
        if (!file.exists()) {
            throw new IllegalArgumentException("input " + args[0] + " does not exist");
        }

        BazelEnv env = new BazelEnv(Arrays.asList(args).subList(1, args.length));
        for (int i = 0; i < 5; i++) {
            System.out.println("Uploading file, loop " + i);
            env.uploadFile(file);
        }
    }
}
