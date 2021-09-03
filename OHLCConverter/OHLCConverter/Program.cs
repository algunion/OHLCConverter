using System;
using System.Globalization;
using System.Threading.Tasks;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.IO;
using Sylvan.Data;

namespace OHLCConverter
{
    class Program
    {
        /// <summary>
        /// The console application can be used in two different modes:
        /// 1) dotnet run --batch-mode true --timeframe 30 --source-dir yourdirpathhere --target-dir yourtargetdir
        /// In "--batch-mode true" you need to also specify the --source-dir and --target-dir arguments.
        /// 2) dotnet run --batch-mode false --timeframe 5 --source-file yourcsvpathhere --target-file optionalcsvtarget
        /// In "--batch-mode false" you need to specify the --source-file (and if you don't specify the --target-file, a file will be produced 
        /// in the working directory using the pattern: symbol$INTRADAYNN.csv).
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
        static async Task<int> Main(string[] args)
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
                    "Session start time: e. g. 9:30"),
                new Option<TimeSpan>(
                    "--session-end",
                    "Session end time: e. g. 16:00")
            };            

            rootCommand.Description = "OHLC bar convertor";
            var p = new Program();
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

                                    foreach (var f in csvFiles)
                                    {
                                        Console.WriteLine($"Processing {f}");
                                        
                                        await p.ConvertCSV(f, timeFrame, sessionStart, sessionEnd, targetDir: targetDir.FullName);
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
                                await p.ConvertCSV(sourceFile.FullName, timeFrame, sessionStart, sessionEnd, targetFile?.FullName ?? "");
                            }
                            else
                            {
                                Console.WriteLine("Please check and use a valid --source-file argument!");
                            }
                        }
                    });

            return rootCommand.InvokeAsync(args).Result;
     

        }
        
        public async Task ConvertCSV(string sourcePath, int timeframe, TimeSpan sessionStart, TimeSpan sessionEnd, string targetPath = "", string targetDir = "")
        {
            Converter converter = new(timeframe, sessionStart, sessionEnd);

            // Sylvan.Data CSV reader is an extremely fast reader using the SIMD approach.
            using var csv = await Sylvan.Data.Csv.CsvDataReader.CreateAsync(sourcePath);            
            
            while (await csv.ReadAsync())
            {
                converter.Convert(readBar());
            }

            await converter.WriteCSVAsync(makeTargetFileName());

            OHLC readBar()
            {
                var date = dateTimeParser(csv.GetString(0));
                var time = timeParser(csv.GetString(1));
                var o = csv.GetDecimal(2);
                var h = csv.GetDecimal(3);
                var l = csv.GetDecimal(4);
                var c = csv.GetDecimal(5);               
                

                // Some volume values are represented by scientific notation in the source files,
                // using NumberStyles.Float fixes the parsing issue.
                // Also, there are volume values that are represented as floating points so I decided to go
                // with deciamal (integer is not a valid option anyway) instead of float to avoid precision related issues.
                var vol = Decimal.Parse(csv.GetString(6), NumberStyles.Float);
                
                var extra1 = csv.GetDecimal(7);      
                var extra2 = csv.GetDecimal(8);
                var extra3 = csv.GetDecimal(9);

                return new()
                {
                    Date = date,
                    OpenTime = time,
                    Open = o,
                    High = h,
                    Low = l,
                    Close = c,
                    Volume = vol,
                    Extra1 = extra1,
                    Extra2 = extra2,
                    Extra3 = extra3
                };
            }            
            
            // helper local functions for custom dataformat and time parsing
            // just for readability purpose             
            DateTime dateTimeParser(string input)
            {                
                return DateTime.ParseExact(input, "yyyyMMdd", CultureInfo.InvariantCulture);
            }

            TimeSpan timeParser(string input)
            {
                return TimeSpan.ParseExact(input.PadLeft(4, '0'), "hhmm", CultureInfo.InvariantCulture);
            }

            string makeTargetFileName()
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

        
}
