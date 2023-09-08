using Microsoft.Spark.Sql;
using System.Collections.Generic;
using System.Linq;
using static Microsoft.Spark.Sql.Functions;

namespace LocalClimateData
{
    internal class Program
    {
        static void Main(string[] args)
        {
            SparkSession sparkSession = SparkSession.Builder()
                                                    .AppName("West Chicago DuPage Airport Local Climatological Data from 2014-01-01 to 2023-09-03")
                                                    .GetOrCreate();


            DataFrame climateDataCsv = sparkSession.Read()
                                                   .Option("header", true)
                                                   .Option("inferSchema", true)
                                                   .Csv("abfss://synw-projects@stprojects.dfs.core.windows.net/LocalClimateData-sparkjob/West-Chicago-DuPage-Airport-Local-Climatological-Data-FROM-2014-01-01-TO-2023-09-03.csv");
            //climateDataCsv.PrintSchema();
            climateDataCsv.Write()
                          .Mode(SaveMode.Overwrite)
                          .Parquet("abfss://synw-projects@stprojects.dfs.core.windows.net/LocalClimateData-sparkjob/West-Chicago-DuPage-Airport-Local-Climatological-Data-FROM-2014-01-01-TO-2023-09-03.parquet");


            DataFrame climateDataParquet = sparkSession.Read()
                                                       .Parquet("abfss://synw-projects@stprojects.dfs.core.windows.net/LocalClimateData-sparkjob/West-Chicago-DuPage-Airport-Local-Climatological-Data-FROM-2014-01-01-TO-2023-09-03.parquet");
            List<string> columnsNotNeeded = new List<string>();
            foreach (string climateDataColumnName in climateDataParquet.Columns())
            {
                long climateDataColumnDistinctValueCount = climateDataParquet.Select(Col(climateDataColumnName))
                                                                             .Distinct()
                                                                             .Count();

                if (climateDataColumnDistinctValueCount == 1 || climateDataColumnDistinctValueCount == 2)
                {
                    // climateDataColumnName either has one distinct value in all rows or some rows
                    columnsNotNeeded.Add(climateDataColumnName);
                }
            }
            // remarks column that contains the raw data used to populate all of the other columns in a row
            columnsNotNeeded.Add("REM");
            // redundant columns
            columnsNotNeeded.AddRange(new string[] { "REPORT_TYPE95", "SOURCE96" });
            DataFrame filteredColumnsClimateDataParquet = climateDataParquet.Drop(columnsNotNeeded.ToArray());
            filteredColumnsClimateDataParquet = filteredColumnsClimateDataParquet.WithColumn("REPORT_TYPE2", Trim(filteredColumnsClimateDataParquet.Col("REPORT_TYPE2")));
            //filteredColumnsClimateDataParquet.PrintSchema();


            string[] monthSummaryColumnStartsWith = new[]
            {
                "Monthly",
                "Short",
                "Normals",
                "AWND",
                "CDSD",
                "CLDD",
                "DYHF",
                "DYTS",
                "HDSD",
                "HTDD"
            };
            string[] dailySummaryColumnStartsWith = new[] { "Daily", "Sunrise", "Sunset" };
            string[] hourlySummaryColumnStartsWith = new[] { "Hourly" };
            List<string> monthSummaryColumnNames = new List<string>();
            List<string> dailySummaryColumnNames = new List<string>();
            List<string> hourlySummaryColumnNames = new List<string>();
            foreach (string columnName in filteredColumnsClimateDataParquet.Columns())
            {
                if (monthSummaryColumnStartsWith.Any(columnStartsWith => columnName.StartsWith(columnStartsWith)))
                {
                    monthSummaryColumnNames.Add(columnName);
                }
                else if (dailySummaryColumnStartsWith.Any(columnStartsWith => columnName.StartsWith(columnStartsWith)))
                {
                    dailySummaryColumnNames.Add(columnName);
                }
                else if (hourlySummaryColumnStartsWith.Any(columnStartsWith => columnName.StartsWith(columnStartsWith)))
                {
                    hourlySummaryColumnNames.Add(columnName);
                }
            }
            DataFrame monthlySummaryClimateData = filteredColumnsClimateDataParquet.Select("DATE", monthSummaryColumnNames.ToArray())
                                                                                   .Where(Col("REPORT_TYPE2") == "SOM");
            DataFrame dailySummaryClimateData = filteredColumnsClimateDataParquet.Select("DATE", dailySummaryColumnNames.ToArray())
                                                                                 .Where(Col("REPORT_TYPE2") == "SOD");
            DataFrame hourlySummaryClimateData = filteredColumnsClimateDataParquet.Select("DATE", hourlySummaryColumnNames.ToArray())
                                                                                  .Where((Col("REPORT_TYPE2") == "FM-15")
                                                                                      .Or(Col("REPORT_TYPE2") == "FM-16"));
            //Console.WriteLine("Total data points: " + filteredColumnsClimateDataParquet.Count());
            //monthlySummaryClimateData.PrintSchema();
            //Console.WriteLine("Total monthly summary data points: " + monthlySummaryClimateData.Count());
            //dailySummaryClimateData.PrintSchema();
            //Console.WriteLine("Total daily summary data points: " + dailySummaryClimateData.Count());
            //hourlySummaryClimateData.PrintSchema();
            //Console.WriteLine("Total hourly summary data points: " + hourlySummaryClimateData.Count());


            monthlySummaryClimateData.WithColumn("MonthlyDaysWithLT0Temp", monthlySummaryClimateData.Col("MonthlyDaysWithLT0Temp").Cast("int"));
            monthlySummaryClimateData.WithColumn("MonthlyGreatestPrecip", monthlySummaryClimateData.Col("MonthlyGreatestPrecip").Cast("double"));
            Dictionary<string, string?> monthlyGroupedColumnsToAliases = new Dictionary<string, string?>()
            {
                { "AWND", "Monthly Average Wind Speed" },
                { "DYHF", "Days with Heavy Fog" },
                { "DYTS", "Days with Thunderstorms" },
                { "MonthlyDaysWithLT0Temp", null },
                { "MonthlyDaysWithGT90Temp", null },
                { "MonthlyMinimumTemperature", null },
                { "MonthlyMaximumTemperature", null },
                { "MonthlyMinSeaLevelPressureValue", null },
                { "MonthlyMaxSeaLevelPressureValue", null },
                { "MonthlyGreatestPrecip", null },
            };
            RelationalGroupedDataset monthlyGroupedByYear = monthlySummaryClimateData.Select("DATE", monthlyGroupedColumnsToAliases.Keys.ToArray())
                                                                                     .GroupBy(Year(Col("DATE")).As("Year"));
            computeAggregationsAndDisplayResults(monthlyGroupedColumnsToAliases, monthlyGroupedByYear, "Year");


            dailySummaryClimateData.WithColumn("DailyMaximumDryBulbTemperature", dailySummaryClimateData.Col("DailyMaximumDryBulbTemperature").Cast("int"));
            dailySummaryClimateData.WithColumn("DailyMinimumDryBulbTemperature", dailySummaryClimateData.Col("DailyMinimumDryBulbTemperature").Cast("int"));
            dailySummaryClimateData.WithColumn("DailyPeakWindSpeed", dailySummaryClimateData.Col("DailyPeakWindSpeed").Cast("int"));
            dailySummaryClimateData.WithColumn("DailyPrecipitation", dailySummaryClimateData.Col("DailyPrecipitation").Cast("double"));
            dailySummaryClimateData.WithColumn("DailySustainedWindSpeed", dailySummaryClimateData.Col("DailySustainedWindSpeed").Cast("int"));
            List<string> dailyGroupedColumns = new List<string>()
            {
                "DailyAverageStationPressure",
                "DailyAverageWindSpeed",
                "DailyMaximumDryBulbTemperature",
                "DailyMinimumDryBulbTemperature",
                "DailyPeakWindSpeed",
                "DailyPrecipitation",
                "DailySustainedWindSpeed"
            };
            RelationalGroupedDataset dailyGroupedByDayOfWeek = dailySummaryClimateData.Select("DATE", dailyGroupedColumns.ToArray())
                                                                                      .GroupBy(DayOfWeek(Col("DATE")).As("DayNumber"));
            computeAggregationsAndDisplayResults(dailyGroupedColumns.ToDictionary(key => key, value => (string?)value), dailyGroupedByDayOfWeek, "DayNumber");


            hourlySummaryClimateData.WithColumn("HourlyDryBulbTemperature", hourlySummaryClimateData.Col("HourlyDryBulbTemperature").Cast("int"));
            hourlySummaryClimateData.WithColumn("HourlyPrecipitation", hourlySummaryClimateData.Col("HourlyPrecipitation").Cast("double"));
            hourlySummaryClimateData.WithColumn("HourlyRelativeHumidity", hourlySummaryClimateData.Col("HourlyRelativeHumidity").Cast("int"));
            hourlySummaryClimateData.WithColumn("HourlySeaLevelPressure", hourlySummaryClimateData.Col("HourlySeaLevelPressure").Cast("double"));
            hourlySummaryClimateData.WithColumn("HourlyStationPressure", hourlySummaryClimateData.Col("HourlyStationPressure").Cast("double"));
            hourlySummaryClimateData.WithColumn("HourlyVisibility", hourlySummaryClimateData.Col("HourlyVisibility").Cast("double"));
            hourlySummaryClimateData.WithColumn("HourlyWindSpeed", hourlySummaryClimateData.Col("HourlyWindSpeed").Cast("int"));
            List<string> hourlyGroupedColumns = new List<string>()
            {
                "HourlyDryBulbTemperature",
                "HourlyPrecipitation",
                "HourlyRelativeHumidity",
                "HourlySeaLevelPressure",
                "HourlyStationPressure",
                "HourlyVisibility",
                "HourlyWindGustSpeed",
                "HourlyWindSpeed"
            };
            RelationalGroupedDataset hourlyGroupedByHour = hourlySummaryClimateData.Select("DATE", hourlyGroupedColumns.ToArray())
                                                                                   .GroupBy(Hour(Col("DATE")).As("Hour"));
            computeAggregationsAndDisplayResults(hourlyGroupedColumns.ToDictionary(key => key, value => (string?)value), hourlyGroupedByHour, "Hour");


            sparkSession.Stop();
        }

        private static void computeAggregationsAndDisplayResults(Dictionary<string, string?> groupedColumnsToAliases, RelationalGroupedDataset groupedDataset, string orderByColumnName)
        {
            List<Column> avgColumns = new List<Column>();
            List<Column> stddevColumns = new List<Column>();
            List<Column> varColumns = new List<Column>();
            foreach (KeyValuePair<string, string?> groupedColumnToAlias in groupedColumnsToAliases)
            {
                string groupedColumnName = groupedColumnToAlias.Key;
                string groupedColumnAlias = groupedColumnToAlias.Value ?? groupedColumnToAlias.Key;

                avgColumns.Add(Round(Avg(groupedColumnName), 2).As($"avg_{groupedColumnAlias}"));
                stddevColumns.Add(Round(StddevPop(groupedColumnName), 2).As($"stddev_{groupedColumnAlias}"));
                varColumns.Add(Round(VarPop(groupedColumnName), 2).As($"var_{groupedColumnAlias}"));
            }
            List<Column>[] aggregates = new[] { avgColumns, stddevColumns, varColumns };

            foreach (List<Column> aggColumns in aggregates)
            {
                groupedDataset.Agg(aggColumns[0], aggColumns.GetRange(1, aggColumns.Count - 1).ToArray())
                              .OrderBy(Col(orderByColumnName).Asc())
                              .Show(numRows: 24);
            }
        }
    }
}
