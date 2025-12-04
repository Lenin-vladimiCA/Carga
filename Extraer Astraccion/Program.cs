using Extraer_Astraccion.ProyectoETLDW.ETL;


    namespace ProyectoETLDW
{
    internal class Program
    {
        static void Main(string[] args)
        {
            var etl = new EtlProcess();
            etl.RunEtl();
        }
    }
}
