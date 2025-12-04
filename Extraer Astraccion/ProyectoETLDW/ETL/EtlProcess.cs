using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Extraer_Astraccion.ProyectoETLDW.ETL
{
    internal class EtlProcess
    {
    
        private const string ConnectionString = "ost=localhost;Username=VentasETL;Password=12345;Database=VentasET";

        private const string CsvFolderPath = @"C:\Users\PC\OneDrive\Desktop\Archivo CSV Análisis de Ventas-20251203\";

        public void RunEtl()
        {
            Console.WriteLine("--- INICIO PROCESO ETL DW ---");

      
            LoadStagingTables();

       
            LoadDimensions();

    
            LoadFactTable();

            Console.WriteLine("--- PROCESO ETL COMPLETADO ---");
        }


        private void LoadStagingTables()
        {
            Console.WriteLine("\n[1/3] Extracción y Carga a Tablas Staging...");

             LoadStagingTable("customers.csv", "Staging_Customer");
            LoadStagingTable("products.csv", "Staging_Product");
            LoadStagingTable("orders.csv", "Staging_Order");
            LoadStagingTable("order_details.csv", "Staging_OrderDetail");
        }

        private void LoadStagingTable(string fileName, string tableName)
        {
            try
            {
           
                string fullFilePath = Path.Combine(CsvFolderPath, fileName);

                if (!File.Exists(fullFilePath))
                {
                    Console.WriteLine($"\t ERROR: Archivo no encontrado en: {fullFilePath}");
                    return;
                }

                using (var conn = new NpgsqlConnection(ConnectionString))
                {
                    conn.Open();

         
                    ExecuteSql(conn, $"TRUNCATE TABLE {tableName} RESTART IDENTITY;");

                 
                    string copyCommand = $"COPY {tableName} FROM '{fullFilePath.Replace(@"\", @"\\")}' WITH (FORMAT CSV, HEADER TRUE)";

                    using (var cmd = new NpgsqlCommand(copyCommand, conn))
                    {
                        cmd.ExecuteNonQuery();
                    }
                    Console.WriteLine($"\t {tableName} cargada correctamente.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\t ERROR fatal al cargar {tableName}: {ex.Message}");
            }
        }

        private void LoadDimensions()
        {
            Console.WriteLine("\n[2/3] Transformación y Carga de Dimensiones...");
            using (var conn = new NpgsqlConnection(ConnectionString))
            {
                conn.Open();

                ExecuteSql(conn, GetCustomerLoadSql());
                Console.WriteLine("\tDim_Customer cargada.");

                ExecuteSql(conn, GetProductLoadSql());
                Console.WriteLine("\t Dim_Product cargada.");

                ExecuteSql(conn, GetDateLoadSql());
                Console.WriteLine("\t Dim_Date cargada.");
            }
        }


        private void LoadFactTable()
        {
            Console.WriteLine("\n[3/3] Transformación y Carga de Tabla de Hechos...");
            using (var conn = new NpgsqlConnection(ConnectionString))
            {
                conn.Open();
                ExecuteSql(conn, GetFactLoadSql());
                Console.WriteLine("\t✅ Fact_OrderDetails cargada.");
            }
        }

        private void ExecuteSql(NpgsqlConnection conn, string sql)
        {
            using (var cmd = new NpgsqlCommand(sql, conn))
            {
                cmd.ExecuteNonQuery();
            }
        }

        private string GetCustomerLoadSql() => @"
            INSERT INTO Dim_Customer (CustomerID, FirstName, LastName, Email, City, Country)
            SELECT DISTINCT 
                sc.CustomerID, sc.FirstName, sc.LastName, sc.Email, sc.City, sc.Country
            FROM Staging_Customer sc
            ON CONFLICT (CustomerID) DO NOTHING;
        ";

        private string GetProductLoadSql() => @"
            INSERT INTO Dim_Product (ProductID, ProductName, Category, Price, Stock)
            SELECT DISTINCT
                sp.ProductID, sp.ProductName, sp.Category, sp.Price, sp.Stock
            FROM Staging_Product sp
            ON CONFLICT (ProductID) DO NOTHING;
        ";

        private string GetDateLoadSql() => @"
            WITH UniqueDates AS (
                SELECT DISTINCT OrderDate AS FullDate FROM Staging_Order
            ),
            DateDetails AS (
                SELECT
                    ud.FullDate,
                    CAST(TO_CHAR(ud.FullDate, 'YYYYMMDD') AS INT) AS DateKey,
                    EXTRACT(DAY FROM ud.FullDate) AS Day,
                    EXTRACT(MONTH FROM ud.FullDate) AS Month,
                    EXTRACT(YEAR FROM ud.FullDate) AS Year,
                    EXTRACT(QUARTER FROM ud.FullDate) AS Quarter,
                    TO_CHAR(ud.FullDate, 'Day') AS DayName,
                    TO_CHAR(ud.FullDate, 'Month') AS MonthName
                FROM UniqueDates ud
            )
            INSERT INTO Dim_Date (DateKey, FullDate, Day, Month, Year, Quarter, DayName, MonthName, IsWeekend)
            SELECT 
                dd.DateKey, dd.FullDate, dd.Day, dd.Month, dd.Year, dd.Quarter, 
                TRIM(dd.DayName), TRIM(dd.MonthName),
                CASE WHEN EXTRACT(DOW FROM dd.FullDate) IN (0, 6) THEN TRUE ELSE FALSE END AS IsWeekend
            FROM DateDetails dd
            ON CONFLICT (FullDate) DO NOTHING;
        ";

        private string GetFactLoadSql() => @"
            INSERT INTO Fact_OrderDetails (
                CustomerKey, ProductKey, DateKey, 
                OrderID, Quantity, UnitPrice, TotalPriceLine, OrderStatus
            )
            SELECT
                dc.CustomerKey,                             -- Clave subrogada del cliente
                dp.ProductKey,                              -- Clave subrogada del producto
                dd.DateKey,                                 -- Clave subrogada de la fecha
                so.OrderID,                                 
                sod.Quantity,                               
                dp.Price AS UnitPrice,                      
                sod.TotalPrice,                             
                so.Status                                   
            FROM 
                Staging_OrderDetail sod
            JOIN 
                Staging_Order so ON sod.OrderID = so.OrderID 
            JOIN 
                Dim_Customer dc ON so.CustomerID = dc.CustomerID 
            JOIN 
                Dim_Product dp ON sod.ProductID = dp.ProductID   
            JOIN 
                Dim_Date dd ON so.OrderDate = dd.FullDate;       
        ";
    }
}
