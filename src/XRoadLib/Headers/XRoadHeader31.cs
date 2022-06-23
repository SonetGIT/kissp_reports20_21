﻿using System.Xml;
using System.Xml.Linq;
using XRoadLib.Extensions;
using XRoadLib.Schema;
using XRoadLib.Serialization;
using XRoadLib.Styles;

namespace XRoadLib.Headers
{
    /// <summary>
    /// Details of X-Road message protocol version 3.1 header.
    /// </summary>
    public class XRoadHeader31 : IXRoadHeader, IXRoadHeader31, IXRoadUniversalHeader
    {
        private XRoadClientIdentifier client = new XRoadClientIdentifier();
        private XRoadServiceIdentifier service = new XRoadServiceIdentifier();

        /// <inheritdoc />
        XRoadClientIdentifier IXRoadHeader.Client => client;

        /// <inheritdoc />
        XRoadServiceIdentifier IXRoadHeader.Service => service;

        /// <summary>
        /// Identifies user who sent X-Road message.
        /// </summary>
        public virtual string UserId { get; set; }

        /// <summary>
        /// Operation specific identifier for the X-Road message.
        /// </summary>
        public virtual string Issue { get; set; }

        /// <inheritdoc />
        string IXRoadHeader.ProtocolVersion => "3.1";

        /// <inheritdoc />
        public virtual string Consumer
        {
            get => client.MemberCode;
            set => client.MemberCode = value;
        }

        /// <inheritdoc />
        public virtual string Producer
        {
            get => service.SubsystemCode;
            set => service.SubsystemCode = value;
        }

        /// <inheritdoc />
        public virtual string ServiceName
        {
            get => service.ToFullName();
            set
            {
                var serviceValue = XRoadServiceIdentifier.FromString(value);
                service.ServiceCode = serviceValue.ServiceCode;
                service.ServiceVersion = serviceValue.ServiceVersion;
            }
        }

        /// <summary>
        /// Unique id for the X-Road message.
        /// </summary>
        public virtual string Id { get; set; }

        /// <inheritdoc cref="IXRoadHeader31" />
        public virtual string Unit { get; set; }

        /// <inheritdoc cref="IXRoadHeader31" />
        public virtual string Position { get; set; }

        /// <inheritdoc cref="IXRoadHeader31" />
        public virtual string UserName { get; set; }

        /// <inheritdoc cref="IXRoadHeader31" />
        public virtual bool? Async { get; set; }

        /// <inheritdoc cref="IXRoadHeader31" />
        public virtual string Authenticator { get; set; }

        /// <inheritdoc cref="IXRoadHeader31" />
        public virtual string Paid { get; set; }

        /// <inheritdoc cref="IXRoadHeader31" />
        public virtual string Encrypt { get; set; }

        /// <inheritdoc cref="IXRoadHeader31" />
        public virtual string EncryptCert { get; set; }

        /// <inheritdoc cref="IXRoadHeader31" />
        public virtual string Encrypted { get; set; }

        /// <inheritdoc cref="IXRoadHeader31" />
        public virtual string EncryptedCert { get; set; }

        /// <inheritdoc />
        public virtual void ReadHeaderValue(XmlReader reader)
        {
            if (reader.NamespaceURI == NamespaceConstants.XROAD)
            {
                switch (reader.LocalName)
                {
                    case "authenticator":
                        Authenticator = reader.ReadElementContentAsString();
                        return;

                    case "userName":
                        UserName = reader.ReadElementContentAsString();
                        return;

                    case "position":
                        Position = reader.ReadElementContentAsString();
                        return;

                    case "unit":
                        Unit = reader.ReadElementContentAsString();
                        return;

                    case "issue":
                        Issue = reader.ReadElementContentAsString();
                        return;

                    case "service":
                        ServiceName = reader.ReadElementContentAsString();
                        return;

                    case "id":
                        Id = reader.ReadElementContentAsString();
                        return;

                    case "userId":
                        UserId = reader.ReadElementContentAsString();
                        return;

                    case "producer":
                        Producer = reader.ReadElementContentAsString();
                        return;

                    case "consumer":
                        Consumer = reader.ReadElementContentAsString();
                        return;

                    case "async":
                        var value = reader.ReadElementContentAsString();
                        Async = !string.IsNullOrWhiteSpace(value) && XmlConvert.ToBoolean(value);
                        return;

                    case "paid":
                        Paid = reader.ReadElementContentAsString();
                        return;

                    case "encrypt":
                        Encrypt = reader.ReadElementContentAsString();
                        return;

                    case "encryptCert":
                        EncryptCert = reader.ReadElementContentAsString();
                        return;

                    case "encrypted":
                        Encrypted = reader.ReadElementContentAsString();
                        return;

                    case "encryptedCert":
                        EncryptedCert = reader.ReadElementContentAsString();
                        return;
                }
            }

            throw new InvalidQueryException($"Unexpected X-Road header element `{reader.GetXName()}`.");
        }

        /// <inheritdoc />
        public virtual void Validate()
        { }

        /// <inheritdoc />
        public virtual void WriteTo(XmlWriter writer, Style style, HeaderDefinition definition)
        {
            if (writer.LookupPrefix(NamespaceConstants.XROAD) == null)
                writer.WriteAttributeString(PrefixConstants.XMLNS, PrefixConstants.XROAD, NamespaceConstants.XMLNS, NamespaceConstants.XROAD);

            void WriteHeaderValue(string elementName, object value, XName typeName)
            {
                var name = XName.Get(elementName, NamespaceConstants.XROAD);
                if (definition.RequiredHeaders.Contains(name) || value != null)
                    style.WriteHeaderElement(writer, name, value, typeName);
            }

            WriteHeaderValue("consumer", Consumer, XmlTypeConstants.String);
            WriteHeaderValue("producer", Producer, XmlTypeConstants.String);
            WriteHeaderValue("userId", UserId, XmlTypeConstants.String);
            WriteHeaderValue("issue", Issue, XmlTypeConstants.String);
            WriteHeaderValue("service", ServiceName, XmlTypeConstants.String);
            WriteHeaderValue("id", Id, XmlTypeConstants.String);
            WriteHeaderValue("unit", Unit, XmlTypeConstants.String);
            WriteHeaderValue("position", Position, XmlTypeConstants.String);
            WriteHeaderValue("userName", UserName, XmlTypeConstants.String);
            WriteHeaderValue("async", Async, XmlTypeConstants.Boolean);
            WriteHeaderValue("authenticator", Authenticator, XmlTypeConstants.String);
            WriteHeaderValue("paid", Paid, XmlTypeConstants.String);
            WriteHeaderValue("encrypt", Encrypt, XmlTypeConstants.String);
            WriteHeaderValue("encryptCert", EncryptCert, XmlTypeConstants.Base64);
            WriteHeaderValue("encrypted", Encrypted, XmlTypeConstants.String);
            WriteHeaderValue("encryptedCert", EncryptedCert, XmlTypeConstants.String);
        }

        XRoadClientIdentifier IXRoadUniversalHeader.Client { get => client; set => client = value; }
        XRoadServiceIdentifier IXRoadUniversalHeader.Service { get => service; set => service = value; }

        string IXRoadUniversalHeader.ProtocolVersion { get => "3.1"; set { } }
    }
}