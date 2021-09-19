using System;
using System.Globalization;
using System.Threading.Tasks;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.IO;
using System.Collections.Generic;
using System.Linq;

namespace OHLCConverter
{
    class Program
    {
        /// <summary>
        /// The console application can be used in three different modes:
        /// 1) dotnet run --batch-mode true --timeframe 30 --source-dir yourdirpathhere --target-dir yourtargetdir
        /// In "--batch-mode true" you need to also specify the --source-dir and --target-dir arguments.
        /// 2) dotnet run --batch-mode false --timeframe 5 --source-file yourcsvpathhere --target-file optionalcsvtarget
        /// In "--batch-mode false" you need to specify the --source-file (and if you don't specify the --target-file, a file will be produced 
        /// in the working directory using the pattern: symbol$INTRADAYNN.csv).
        /// 
        /// I created some mockup implementations for the provided interfaces - just to be able to test the app
        ///
        ///
        /// Please keep in mind that in order to identify the symbol in the filename, the current application uses the _ (underscore) character
        /// to split the name and extract the symbol part - I didn't try/catch potential issues in the filename.
        /// 
        /// If you want to use the startSession and endSession, this are valid commands:
        /// dotnet run --batch-mode true --timeframe 30 --source-dir yourdirpathhere --target-dir yourtargetdir --session-start 10:00 --session-end 22:00
        /// 
        /// </summary>
        /// <param name="args"></param>
        /// <returns></returns>
        static int Main(string[] args)
        {
            var rootCommand = new RootCommand
            {
                new Option<int>(
                    "--timeframe",
                    getDefaultValue: () => 30,
                    description: "New target timeframe value (in minutes)."),
                new Option<bool>(
                    "--batch-mode",
                    getDefaultValue: () => false,
                    description: "Running the convertor in batch mode (need to specify the source folder)"),
                new Option<FileInfo>(
                    "--source-dir",
                    "Directory containing the CSV files."),
                new Option<FileInfo>(
                    "--target-dir",
                    "Directory to contain converted CSV files (it will be created if does not exist)."),
                new Option<FileInfo>(
                    "--source-file",
                    "CSV file to convert."),
                new Option<FileInfo>(
                    "--target-file",
                    "CSV path for the converted file (default: saved locally using the symbol$INTRADAYNN.csv pattern)."),
                new Option<TimeSpan>(
                    "--session-start",
                    getDefaultValue: () => new TimeSpan(9, 30, 0),
                    description: "Session start time: e. g. 9:30"),
                new Option<TimeSpan>(
                    "--session-end",
                    getDefaultValue: () => new TimeSpan(16, 00, 0),
                    description: "Session end time: e. g. 16:00")
            };

            rootCommand.Description = "OHLC bar convertor";
            rootCommand.Handler =
                CommandHandler.Create<int, bool, FileInfo, FileInfo, FileInfo, FileInfo, TimeSpan, TimeSpan>(
                    async (timeFrame, batchMode, sourceDir, targetDir, sourceFile, targetFile, sessionStart, sessionEnd) =>
                    {
                        Console.WriteLine($"--timeframe is: {timeFrame}");
                        Console.WriteLine($"--batch-mode is: {batchMode}");
                        Console.WriteLine($"--source-dir is: {sourceDir?.FullName ?? null}");
                        Console.WriteLine($"--target-dir is: {targetDir?.FullName ?? null}");
                        Console.WriteLine($"--source-file is: {sourceFile?.FullName ?? null}");
                        Console.WriteLine($"--target-file is: {targetFile?.FullName ?? null}");
                        Console.WriteLine($"--session-start is: {sessionStart}");
                        Console.WriteLine($"--session-end is: {sessionEnd}");                        
                        Console.WriteLine();

                        if (batchMode)
                        {
                            if (sourceDir is not null && targetDir is not null)
                            {
                                if (!Directory.Exists(sourceDir.FullName))
                                {
                                    Console.WriteLine($"--source-dir value is not referring to an existing directory: {sourceDir.FullName}");
                                }
                                else
                                {
                                    if (!targetDir.Exists) Directory.CreateDirectory(targetDir.FullName);

                                    var csvFiles = Array.FindAll(Directory.GetFiles(sourceDir.FullName), path => Path.GetExtension(path) == ".csv");

                                    List<double> elapsed = new List<double>();
                                    foreach (var f in csvFiles)
                                    {
                                        DateTime now = DateTime.Now;                                        
                                        Console.WriteLine($"Processing {f}");
                                        await PseudoEOD(f, timeFrame, sessionStart, sessionEnd, targetDir: targetDir.FullName);
                                        var end = DateTime.Now.Subtract(now).TotalSeconds;
                                        Console.WriteLine($"Elapsed: {end}");
                                        elapsed.Add(end);
                                        double avg = Queryable.Average(elapsed.AsQueryable());
                                        Console.WriteLine($"mean: {avg}");
                                        Console.WriteLine("-----------------");


                                    }
                                }
                            }
                            else
                            {
                                Console.WriteLine("Please check --source-dir and --target-dir values!");
                            }
                        }
                        else
                        {
                            if (sourceFile is not null && sourceFile.Exists)
                            {
                                Console.WriteLine($"Processing {sourceFile.FullName}");
                                await PseudoEOD(sourceFile.FullName, timeFrame, sessionStart, sessionEnd);
                            }
                            else
                            {
                                Console.WriteLine("Please check and use a valid --source-file argument!");
                            }
                        }
                    });

            return rootCommand.InvokeAsync(args).Result;

        }
        
        public static async Task PseudoEOD(string sourcePath, int timeframe, TimeSpan sessionStart, TimeSpan sessionEnd, string targetPath = "", string targetDir = "")
        {
            var dataManager = new DataManager(timeframe, sessionStart, sessionEnd, DateTime.Now.Date, new TradingDateService());
            var batchSaver = new ServerEntityBatchSaver();
            
            Console.WriteLine("Creating Pseudo EOD data...");
            await dataManager.CreatePseudoEODDataFromMinuteBarCsvAsync(sourcePath, 10, batchSaver);
            
            Console.WriteLine("Writing CSV file...");
            await batchSaver.DropToCsv(MakeTargetFileName(sourcePath, timeframe, targetDir, targetPath));
            Console.WriteLine("Done...");
        }

        static string MakeTargetFileName(string sourcePath, int timeframe, string targetDir = "", string targetPath = "")
        {
            var symbol = Path.GetFileNameWithoutExtension(sourcePath).Split("_")[1];
            var fileName = $"{symbol.ToUpper()}$INTRADAY{timeframe}.csv";

            if (!String.IsNullOrEmpty(targetDir))
            {
                return Path.Combine(targetDir, fileName);
            }
            else if (!String.IsNullOrEmpty(targetPath))
            {
                return targetPath;
            }

            return fileName;
        }
    }

        
}
