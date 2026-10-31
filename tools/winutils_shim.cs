// Minimal local stand-in for Hadoop's winutils.exe (chmod/chown no-ops, `ls -F` metadata).
// Lets Spark Structured Streaming create checkpoint directories on a Windows machine
// without downloading third-party binaries.  Build: see README.
using System;
using System.IO;

class WinUtils {
    static int Main(string[] a) {
        if (a.Length == 0) return 0;
        if (a[0] == "ls") {
            string p = a[a.Length - 1];
            bool dir = Directory.Exists(p);
            long size = 0;
            if (!dir && File.Exists(p)) size = new FileInfo(p).Length;
            Console.WriteLine((dir ? "d" : "-") + "rwxrwxrwx|1|user|group|" + size + "|Jan 1 2020|" + p);
            return 0;
        }
        return 0;   // chmod, chown, ... are accepted and ignored
    }
}
