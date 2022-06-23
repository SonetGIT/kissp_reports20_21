using System.Reflection;
using XRoadLib;
using XRoadLib.Headers;
using XRoadLib.Schema;

namespace WebApplication
{
    public class CalculatorServiceManager : ServiceManager<XRoadHeader40>
    {
        public CalculatorServiceManager()
            : base("4.0", new DefaultSchemaExporter("http://calculator.x-road.eu/", typeof(CalculatorServiceManager).GetTypeInfo().Assembly))
        { }
    }
}