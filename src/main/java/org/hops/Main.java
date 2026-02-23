package org.hops;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class Main {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            printUsage();
            System.exit(1);
        }

        String keytabPath = System.getenv("KEYTAB_PATH");
        String principal = System.getenv("KRB_PRINCIPAL");
        String krb5ConfPath = System.getenv("KRB5_CONF_PATH");

        if (keytabPath == null || principal == null) {
            System.err.println("Environment variables KEYTAB_PATH and KRB_PRINCIPAL must be set.");
            System.exit(1);
        }

        if (krb5ConfPath != null) {
            System.setProperty("java.security.krb5.conf", krb5ConfPath);
        }

        Configuration conf = new Configuration();
        conf.addResource(new Path("core-site.xml"));
        conf.addResource(new Path("hdfs-site.xml"));

        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(principal, keytabPath);
        System.out.println("Authenticated as: " + UserGroupInformation.getCurrentUser());

        FileSystem fs = FileSystem.get(conf);

        String command = args[0];
        switch (command) {
            case "ls":
                listDirectory(fs, args.length > 1 ? args[1] : "/");
                break;
            case "mkdir":
                requireArg(args, 2, "mkdir <path>");
                mkdirs(fs, args[1]);
                break;
            case "put":
                requireArg(args, 3, "put <content> <hdfs-path>");
                writeFile(fs, args[2], args[1]);
                break;
            case "cat":
                requireArg(args, 2, "cat <hdfs-path>");
                readFile(fs, args[1]);
                break;
            case "rm":
                requireArg(args, 2, "rm <hdfs-path>");
                deleteFile(fs, args[1]);
                break;
            default:
                System.err.println("Unknown command: " + command);
                printUsage();
                System.exit(1);
        }

        fs.close();
    }

    private static void listDirectory(FileSystem fs, String path) throws Exception {
        Path hdfsPath = new Path(path);
        if (!fs.exists(hdfsPath)) {
            System.err.println("Path does not exist: " + path);
            return;
        }
        FileStatus[] statuses = fs.listStatus(hdfsPath);
        for (FileStatus status : statuses) {
            System.out.printf("%s %10d %s%n",
                    status.isDirectory() ? "d" : "-",
                    status.getLen(),
                    status.getPath().toString());
        }
    }

    private static void mkdirs(FileSystem fs, String path) throws Exception {
        Path hdfsPath = new Path(path);
        if (fs.mkdirs(hdfsPath)) {
            System.out.println("Created directory: " + path);
        } else {
            System.err.println("Failed to create directory: " + path);
        }
    }

    private static void writeFile(FileSystem fs, String hdfsPathStr, String content) throws Exception {
        Path hdfsPath = new Path(hdfsPathStr);
        try (FSDataOutputStream out = fs.create(hdfsPath, true)) {
            out.write(content.getBytes(StandardCharsets.UTF_8));
        }
        System.out.println("Written to: " + hdfsPathStr);
    }

    private static void readFile(FileSystem fs, String path) throws Exception {
        Path hdfsPath = new Path(path);
        if (!fs.exists(hdfsPath)) {
            System.err.println("File does not exist: " + path);
            return;
        }
        try (FSDataInputStream in = fs.open(hdfsPath);
             BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        }
    }

    private static void deleteFile(FileSystem fs, String path) throws Exception {
        Path hdfsPath = new Path(path);
        if (fs.delete(hdfsPath, true)) {
            System.out.println("Deleted: " + path);
        } else {
            System.err.println("Failed to delete: " + path);
        }
    }

    private static void requireArg(String[] args, int minCount, String usage) {
        if (args.length < minCount) {
            System.err.println("Usage: " + usage);
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.out.println("HDFS Client - Basic Operations");
        System.out.println();
        System.out.println("Environment variables:");
        System.out.println("  KEYTAB_PATH    - Path to the Kerberos keytab file");
        System.out.println("  KRB_PRINCIPAL  - Kerberos principal (e.g. user@HOPSWORKS.AI)");
        System.out.println("  KRB5_CONF_PATH - (Optional) Path to krb5.conf");
        System.out.println();
        System.out.println("Commands:");
        System.out.println("  ls [path]              - List directory (default: /)");
        System.out.println("  mkdir <path>           - Create directory");
        System.out.println("  put <content> <path>   - Write text content to a file");
        System.out.println("  cat <path>             - Read a file");
        System.out.println("  rm <path>              - Delete a file or directory");
    }
}
