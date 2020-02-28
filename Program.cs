using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data.SqlClient;
using System.Data;
using System.Diagnostics;

namespace DanNetCSVConverter
{
    class Program
    {
        static void Main(string[] args)
        {
            string folderPath = Path.Combine("Libs","DanNet-2.2_csv");
            ConvertCsvsToSql(folderPath);
        }

        static bool ConvertCsvsToSql(string folderPath)
        {
            if (Directory.Exists(folderPath) == false)
            {
                return false;
            }
            List<string> fileNames = Directory.GetFiles(folderPath).Select(x => x).Where(x => x.Contains(".csv")).ToList();
            if (fileNames.Count() == 0)
            {
                return false;
            }
            foreach (string fileName in fileNames)
            {
                var csvValues = File.ReadAllLines(fileName, Encoding.GetEncoding("iso-8859-1"));
                string fileNameWOExtention = Path.GetFileNameWithoutExtension(fileName);
                string tableName = "";
                string[] columnNames;
                string[] columnTypes;
                string outputPath = "Output";
                Directory.CreateDirectory(outputPath);
                switch (fileNameWOExtention.ToLower())
                {
                    //case "dummies":
                    //    columnNames = new string[] { "id", "label", "gloss", "ontological_type" };
                    //    columnTypes = new string[] { "varchar(255)", "varchar(255)", "varchar(8000)", "varchar(255)" };
                    //    tableName = "Dummy";
                    //    break;
                    case "relations":
                        columnNames = new string[] { "FromSynsetId", "Name", "Name2", "ToSynsetId", "Taxonomic", "Inheritance_comment" };
                        columnTypes = new string[] { "varchar(255)", "varchar(255)", "varchar(255)", "varchar(255)", "varchar(8000)", "varchar(255)" };
                        tableName = "Relation";
                        break;
                    //case "synset_attributes":
                    //    // File.WriteAllText(Path.Combine(folderPath, fileNameWOExtention + DateTime.Now.ToString("yyyyMMddTHHmmss") + ".sql"), CreateSql(tableName, new string[] { "", "" }, new string[] { "", "" }, csvValues);
                    //    break;
                    case "synsets":
                        columnNames = new string[] { "Id", "Label", "Gloss", "Ontological_type" };
                        columnTypes = new string[] { "varchar(255) PRIMARY KEY", "varchar(255)", "varchar(8000)", "varchar(255)" };
                        tableName = "Synset";
                        break;
                    case "wordsenses":
                        columnNames = new string[] { "Id", "WordId", "SynsetId", "Register" };
                        columnTypes = new string[] { "varchar(255)", "varchar(255)", "varchar(255)", "varchar(255)" };
                        tableName = "Wordsense";
                        break;
                    case "words":
                        columnNames = new string[] { "Id", "LexicalForm", "Pos" };
                        columnTypes = new string[] { "varchar(255)", "varchar(255)", "varchar(255), CONSTRAINT [PK_WORD] PRIMARY KEY CLUSTERED ( [Id] ASC ) WITH (IGNORE_DUP_KEY = OFF)" };
                        tableName = "Word";
                        break;
                    default:
                        continue;
                }
                int maxValuesPrSql = 100000;
                int count = 0;
                while (csvValues.Length > 0)
                {
                    string modifier = "Part" + count;
                    string[] values;
                    if (csvValues.Length > maxValuesPrSql)
                    {
                        values = csvValues.Take(maxValuesPrSql).ToArray();
                    }
                    else
                    {
                        if (count == 0)
                        {
                            modifier = "";
                        }
                        values = csvValues;
                    }

                    File.WriteAllText(Path.Combine(outputPath, fileNameWOExtention + DateTime.Now.ToString("yyyyMMddTHHmmss") + modifier + ".sql"), CreateSql(tableName, columnNames, columnTypes, values), Encoding.UTF8);
                    csvValues = csvValues.Skip(maxValuesPrSql).ToArray();
                    count++;
                }
            }
            return true;
        }
        static string CreateSql(string tableName, string[] columns, string[] columnTypes, string[] csvValues)
        {
            int maximumRowCount = 1000;
            var columnsCount = columns.Length;
            StringBuilder sql = new StringBuilder();
            sql.AppendFormat("IF NOT EXISTS ( SELECT * FROM sys.tables where name = '{0}')", tableName);
            sql.AppendFormat("\nCREATE TABLE {0} (", tableName);
            for (int i = 0; i < columnsCount; i++)
            {
                if (i > 0)
                {
                    sql.Append(",");
                }
                sql.Append("[" + columns[i] + "] " + columnTypes[i]);
            }
            sql.Append(");");
            //sqlList.Add(sql);
            string insertIntoSql = "";
            insertIntoSql += string.Format("\n\nINSERT INTO {0} (", tableName);
            for (int i = 0; i < columnsCount; i++)
            {
                if (i > 0)
                {
                    insertIntoSql += ",";
                }
                insertIntoSql += string.Format("[{0}]", columns[i]);
            }

            // sql = "";
            DateTime startTime = DateTime.Now;
            for (int i = 0; i < csvValues.Length; i++)
            {
                var valuesBeforeAdd = csvValues[i].Split('@');
                if (i % maximumRowCount == 0)
                {
                    sql.Append("");
                    sql.Append(insertIntoSql);
                    sql.Append(")");
                    sql.Append("\nVALUES");
                }
                if (i % maximumRowCount != 0)
                {
                    sql.Append(",");
                }
                sql.Append("\n(");
                for (int ii = 0; ii < columnsCount; ii++)
                {
                    if (ii > 0)
                    {
                        sql.Append(",");
                    }
                    sql.AppendFormat("'{0}'", valuesBeforeAdd[ii].Replace("'", ""));
                }
                sql.Append(")");
                if ((i + 1) % maximumRowCount == 0 && i != 0 || i == csvValues.Length - 1)
                {
                    sql.Append(";");
                }
                drawTextProgressBar(i + 1, csvValues.Length, startTime);

            }


            return sql.ToString();
        }
        private static void drawTextProgressBar(int count, int total, DateTime startTime)
        {
            //draw empty progress bar
            Console.CursorLeft = 0;
            Console.Write("["); //start
            Console.CursorLeft = 32;
            Console.Write("]"); //end
            Console.CursorLeft = 1;
            float onechunk = 30.0f / total;

            //draw filled part
            int position = 1;
            for (int i = 0; i < onechunk * count; i++)
            {
                Console.BackgroundColor = ConsoleColor.Gray;
                Console.CursorLeft = position++;
                Console.Write(" ");
            }

            //draw unfilled part
            for (int i = position; i <= 31; i++)
            {
                Console.BackgroundColor = ConsoleColor.Green;
                Console.CursorLeft = position++;
                Console.Write(" ");
            }

            //draw totals
            Console.CursorLeft = 35;
            Console.BackgroundColor = ConsoleColor.Black;

            TimeSpan timePassed = DateTime.Now - startTime;
            double countPrTick = count / (double)timePassed.Ticks;
            double estimatedTicks = total / countPrTick;
            double estimatedTicksRemaining = estimatedTicks - timePassed.Ticks;
            TimeSpan timeRemaining = TimeSpan.FromTicks((long)estimatedTicksRemaining); // TimeSpan.FromTicks(DateTime.Now.Subtract(startTime).Ticks * (total - (count + 1)) / (count + 1));
            Console.Write(count.ToString() + " of " + total.ToString() + "    " + timeRemaining); //blanks at the end remove any excess
        }
    }
}
