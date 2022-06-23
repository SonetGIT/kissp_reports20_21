﻿using System.IO;
using System.Xml;
using XRoadLib.Schema;
using XRoadLib.Serialization.Template;

namespace XRoadLib.Serialization.Mapping
{
    public class OptimizedContentTypeMap : ITypeMap
    {
        public TypeDefinition Definition { get; }

        public OptimizedContentTypeMap(ContentTypeMap contentTypeMap)
        {
            Definition = contentTypeMap.Definition;
        }

        public object Deserialize(XmlReader reader, IXmlTemplateNode templateNode, ContentDefinition content, XRoadMessage message)
        {
            if (!reader.ReadToDescendant("Include", NamespaceConstants.XOP))
                throw new InvalidQueryException("Missing `xop:Include` reference to multipart content.");

            var contentID = reader.GetAttribute("href");
            if (string.IsNullOrWhiteSpace(contentID))
                throw new InvalidQueryException("Missing `href` attribute to multipart content.");

            var attachment = message.GetAttachment(contentID.Substring(4));
            if (attachment == null)
                throw new InvalidQueryException($"MIME multipart message does not contain message part with ID `{contentID}`.");

            return attachment.ContentStream;
        }

        public void Serialize(XmlWriter writer, IXmlTemplateNode templateNode, object value, ContentDefinition content, XRoadMessage message)
        {
            var attachment = new XRoadAttachment((Stream)value);
            message.AllAttachments.Add(attachment);

            message.Style.WriteType(writer, Definition, content);

            writer.WriteStartElement(PrefixConstants.XOP, "Include", NamespaceConstants.XOP);

            writer.WriteAttributeString("href", $"cid:{attachment.ContentID}");

            writer.WriteEndElement();
        }
    }
}