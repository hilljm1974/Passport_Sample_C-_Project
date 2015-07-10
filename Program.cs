using LumenWorks.Framework.IO.Csv;
using System;
using System.Data.SqlClient;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Text;
using System.Threading;

namespace PassportSample1
{
    class Program
    {
        static void DownLoadZip()
        {
            using (var client = new WebClient())
            {
                try
                {
                    client.DownloadFile("http://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/Provider-of-Services/Downloads/MAR15_OTHER_CSV.zip", AppDomain.CurrentDomain.BaseDirectory + @"Processing\POS_Download.zip");
                }
                catch(Exception ex)
                {
                    DisplayMessage(ConsoleColor.Red, Environment.NewLine + ex.Message);
                }
            }
        }

        static void UnZipDownload()
        {
            try
            {
                ZipFile.ExtractToDirectory(AppDomain.CurrentDomain.BaseDirectory + @"Processing\POS_Download.zip", AppDomain.CurrentDomain.BaseDirectory + @"Processing\");
            }
            catch(Exception ex)
            {
                DisplayMessage(ConsoleColor.Red, Environment.NewLine + ex.Message);
            }
        }

        //replaced with next version since data contains inconsistent quoting!
        static void LoadDownloadCSV_BulkInsert()
        {
            string connStr = @"Server=LISA-DT\SQLEXPRESS;Database=Passport;Trusted_Connection=true";

            try
            {
                SqlConnection conn = new SqlConnection(connStr);
                SqlCommand sqlcmd = new SqlCommand();
                sqlcmd.Connection = conn;

                conn.Open();

                try
                {
                    //replaced since this hide any errors during the insert
                    /*
                    sqlcmd.CommandText = @"BEGIN TRANSACTION
                    BEGIN TRY
                    BULK INSERT dbo.CSV_Temp_Table
                    FROM '" + AppDomain.CurrentDomain.BaseDirectory + @"Processing\POS_OTHER_MAR15.csv'
                    WITH
                    (
                        FIELDTERMINATOR = ',',
                        ROWTERMINATOR = '\n',
                        ROWS_PER_BATCH = 10000, 
                        TABLOCK
                    )
                    COMMIT TRANSACTION
                    END TRY
                    BEGIN CATCH
                    ROLLBACK TRANSACTION
                    END CATCH";
                    */

                    sqlcmd.CommandTimeout = 180;
                    sqlcmd.CommandText = @"
                    TRUNCATE TABLE dbo.Extended_CSV_Temp_Table
                    BULK INSERT dbo.Extended_CSV_Temp_Table
                    FROM '" + AppDomain.CurrentDomain.BaseDirectory + @"Processing\POS_OTHER_MAR15.csv'
                    WITH
                    (
                        FIRSTROW = 2,
                        FIELDTERMINATOR = ',',
                        ROWS_PER_BATCH = 10000, 
                        TABLOCK
                    )";

                    sqlcmd.ExecuteNonQuery();
                }
                catch(Exception ex)
                {
                    DisplayMessage(ConsoleColor.Red, Environment.NewLine + ex.Message);
                }
                finally
                {
                    conn.Close();
                }

                Directory.Delete(AppDomain.CurrentDomain.BaseDirectory + @"Processing\", true);
            }
            catch (Exception ex)
            {
                DisplayMessage(ConsoleColor.Red, Environment.NewLine + ex.Message);
            }
        }

        static void LoadDownloadCSV_SQLBulkCopy()
        {
            string connStr = @"Server=LISA-DT\SQLEXPRESS;Database=Passport;Trusted_Connection=true";

            try
            {
                SqlConnection conn = new SqlConnection(connStr);
                conn.Open();
                SqlTransaction transaction = conn.BeginTransaction();
                try
                {
                    // Delete old entries
                    SqlCommand truncTable = new SqlCommand("TRUNCATE TABLE dbo.Extended_CSV_Temp_Table", conn, transaction);
                    truncTable.ExecuteNonQuery();

                    using (StreamReader file = new StreamReader(AppDomain.CurrentDomain.BaseDirectory + @"Processing\POS_OTHER_MAR15.csv"))
                    {
                        CsvReader csv = new CsvReader(file, true, ',');
                        SqlBulkCopy copy = new SqlBulkCopy(conn, SqlBulkCopyOptions.TableLock, transaction);
                        copy.BulkCopyTimeout = 180;
                        copy.DestinationTableName = "dbo.Extended_CSV_Temp_Table";
                        copy.WriteToServer(csv);
                        transaction.Commit();
                    }
                }
                catch (Exception ex)
                {
                    transaction.Rollback();
                    DisplayMessage(ConsoleColor.Red, Environment.NewLine + ex.Message);
                }
                finally
                {
                    conn.Close();
                }

                Directory.Delete(AppDomain.CurrentDomain.BaseDirectory + @"Processing\", true);
            }
            catch(Exception ex)
            {
                DisplayMessage(ConsoleColor.Red, Environment.NewLine + ex.Message);
            }
        }

        static void Main(string[] args)
        {
            bool bFailed = false;
            string[] CurArr = new string[4];
            int idx = 0;

            CurArr[0] = @"\";
            CurArr[1] = "|";
            CurArr[2] = "/";
            CurArr[3] = "-";

            int ProcessID = 0;

            string[] Operation = new string[3];
            Operation[0] = "Downloading zip file";
            Operation[1] = "Unzipping download";
            Operation[2] = "Loading Database with CSV file";

            ThreadStart[] ThreadFunction = new ThreadStart[3];
            ThreadFunction[0] = new ThreadStart(DownLoadZip);
            ThreadFunction[1] = new ThreadStart(UnZipDownload);
            //ThreadFunction[2] = new ThreadStart(LoadDownloadCSV_BulkInsert);
            ThreadFunction[2] = new ThreadStart(LoadDownloadCSV_SQLBulkCopy);

            Thread[] WorkerThread = new Thread[3];
            WorkerThread[0] = new Thread(ThreadFunction[0]);
            WorkerThread[1] = new Thread(ThreadFunction[1]);
            WorkerThread[2] = new Thread(ThreadFunction[2]);

            if(Directory.Exists(AppDomain.CurrentDomain.BaseDirectory + @"\Processing\") == false)
            {
                Directory.CreateDirectory(AppDomain.CurrentDomain.BaseDirectory + @"\Processing\");
            }

            while (ProcessID < 3 && !bFailed)
            {
                if (!WorkerThread[ProcessID].IsAlive) { WorkerThread[ProcessID].Start(); }

                DisplayMessage(ConsoleColor.Yellow, Operation[ProcessID] + " - press Esc to terminate current operation ", false);

                while (WorkerThread[ProcessID].ThreadState == ThreadState.Running)
                {
                    if (Console.KeyAvailable)
                    {
                        if (Console.ReadKey(true).Key == ConsoleKey.Escape)
                        {
                            DisplayMessage(ConsoleColor.Blue, Environment.NewLine + "Please wait... aborting [" + Operation[ProcessID] + "]");
                            WorkerThread[ProcessID].Abort();
                            bFailed = true;
                        }
                    }
                    else
                    {
                        DisplayMessage(ConsoleColor.White, CurArr[idx++], false);
                        Console.SetCursorPosition(Console.CursorLeft - 1, Console.CursorTop);
                        idx %= 4;
                        Thread.Sleep(50); //small time delay for spinner effect :-)
                    }
                }

                DisplayMessage(ConsoleColor.Black, " ");
                if (!bFailed)
                {
                    switch (ProcessID)
                    {
                        case 0:
                            if (File.Exists(AppDomain.CurrentDomain.BaseDirectory + @"Processing\POS_Download.zip"))
                            {
                                DisplayMessage(ConsoleColor.Yellow, "Download complete");
                            }
                            else
                            {
                                DisplayMessage(ConsoleColor.Red, "Download failed");
                                bFailed = true;
                            }
                            break;
                        case 1:
                            if (Directory.GetFiles(AppDomain.CurrentDomain.BaseDirectory + @"Processing\", "*.csv").Length > 0)
                            {
                                DisplayMessage(ConsoleColor.Yellow, "Unzipping completed");
                            }
                            else
                            {
                                DisplayMessage(ConsoleColor.Red, "unzip failed");
                                bFailed = true;
                            }
                            break;
                        case 2:
                            if (!Directory.Exists(AppDomain.CurrentDomain.BaseDirectory + @"Processing\"))
                            {
                                DisplayMessage(ConsoleColor.Yellow, "Loading database with CSV file completed");
                            }
                            else
                            {
                                DisplayMessage(ConsoleColor.Red, "Loading database with CSV file failed");
                                bFailed = true;
                            }
                            break;
                    }

                    ProcessID++;
                }
            }

            if (!bFailed) { DisplayMessage(ConsoleColor.Yellow, "Operations completed... ", false); }

            DisplayMessage(ConsoleColor.Yellow, "Press any key to continue");
            Console.ReadKey();
        }

        static void DisplayMessage(System.ConsoleColor color, string Message, bool bNewLine = true)
        {
            Console.ForegroundColor = color;
            if(bNewLine)
            {
                Console.WriteLine(Message);
            }
            else
            {
                Console.Write(Message);
            }

            //TODO: Future expansion - write this information to a log fie / log table
        }
    }
}
