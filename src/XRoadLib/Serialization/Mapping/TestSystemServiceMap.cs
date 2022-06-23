﻿using System.Reflection;
using System.Xml;
using System.Xml.Linq;
using XRoadLib.Extensions;
using XRoadLib.Schema;

namespace XRoadLib.Serialization.Mapping
{
    public class TestSystemServiceMap : IServiceMap
    {
        public OperationDefinition OperationDefinition { get; }
        public RequestDefinition RequestDefinition { get; }
        public ResponseDefinition ResponseDefinition { get; }

        public TestSystemServiceMap(XName operationName)
        {
            var methodInfo = typeof(Implementation).GetTypeInfo().GetMethod("Execute");

            OperationDefinition = new OperationDefinition(operationName, null, methodInfo);
            RequestDefinition = new RequestDefinition(OperationDefinition, _ => false);
            ResponseDefinition = new ResponseDefinition(OperationDefinition, _ => false) { ContainsNonTechnicalFault = true };
        }

        public object DeserializeRequest(XmlReader reader, XRoadMessage message)
        {
            throw new System.NotImplementedException();
        }

        public object DeserializeResponse(XmlReader reader, XRoadMessage message)
        {
            throw new System.NotImplementedException();
        }

        public void SerializeRequest(XmlWriter writer, object value, XRoadMessage message, string requestNamespace)
        {
            throw new System.NotImplementedException();
        }

        public void SerializeResponse(XmlWriter writer, object value, XRoadMessage message, XmlReader requestReader, ICustomSerialization customSerialization = null)
        {
            var containsRequest = requestReader.MoveToElement(2, OperationDefinition.Name);

            if (containsRequest)
                writer.WriteStartElement(requestReader.Prefix, $"{OperationDefinition.Name.LocalName}Response", OperationDefinition.Name.NamespaceName);
            else writer.WriteStartElement($"{OperationDefinition.Name.LocalName}Response", OperationDefinition.Name.NamespaceName);

            writer.WriteEndElement();
        }

        private class Implementation
        {
            // ReSharper disable once UnusedMember.Local
            public void Execute()
            { }
        }
    }
}