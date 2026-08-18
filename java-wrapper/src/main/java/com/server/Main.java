package com.server;

import java.io.*;
import java.nio.file.*;

public class Main {
    public static void main(String[] args) throws Exception {
        String arch = System.getProperty("os.arch").toLowerCase();
        String binaryName;
        if (arch.contains("aarch64") || arch.contains("arm64")) {
            binaryName = "web-linux-arm64";
        } else {
            binaryName = "web-linux-amd64";
        }

        // 从 JAR 内解压 Rust 二进制到临时目录
        File tmpDir = new File(System.getProperty("java.io.tmpdir"), "rust-server");
        tmpDir.mkdirs();
        File binaryFile = new File(tmpDir, "web");
        extractResource("/natives/" + binaryName, binaryFile);
        binaryFile.setExecutable(true);

        // 解压 index.html 到工作目录
        File indexHtml = new File("index.html");
        extractResource("/index.html", indexHtml);

        // 启动 Rust 进程，透传所有环境变量
        ProcessBuilder pb = new ProcessBuilder(binaryFile.getAbsolutePath());
        pb.inheritIO();
        pb.directory(new File(".").getAbsoluteFile());
        pb.environment().putAll(System.getenv());

        Process process = pb.start();
        int exitCode = process.waitFor();
        System.exit(exitCode);
    }

    private static void extractResource(String resourcePath, File dest) throws IOException {
        try (InputStream is = Main.class.getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new FileNotFoundException("Resource not found: " + resourcePath);
            }
            Files.copy(is, dest.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
