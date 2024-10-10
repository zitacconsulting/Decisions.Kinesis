using System;
using System.Runtime.Serialization;
using DecisionsFramework.Data.ORMapper;
using DecisionsFramework.Design.ConfigurationStorage.Attributes;
using DecisionsFramework.Design.Properties;
using DecisionsFramework.ServiceLayer;
using DecisionsFramework.ServiceLayer.Actions;
using DecisionsFramework.ServiceLayer.Utilities;
using DecisionsFramework.Utilities;

namespace Decisions.KinesisPayloadFilter
{
    [ORMEntity("kinesis_payloadfilter")]
    [DataContract]
    public class PayloadFilter : AbstractEntity
    {
        [ORMPrimaryKeyField]
        [PropertyHidden]
        [DataMember]
        public string Id { get; set; }

        [ORMField("property")]
        [DataMember]
        [WritableValue]
        public string Property { get; set; }

        [ORMField("filterverb")]
        [DataMember]
        [WritableValue]
        [SelectStringEditor("FilterVerbs")]
        public string Verb { get; set; }

        [ORMField("value")]
        [DataMember]
        [WritableValue]
        public string Value { get; set; }

        [PropertyHidden]
        public string[] FilterVerbs
        {
            get
            {
                return new string[]
                {
                    "Equals",
                    "Equals (Case Insensitive)",
                    "Not Equals",
                    "Not Equals (Case Insensitive)",
                    "Contains",
                    "Contains (Case Insensitive)",
                    "Starts With",
                    "Starts With (Case Insensitive)",
                    "Ends With",
                    "Ends With (Case Insensitive)",
                    "Greater Than",
                    "Greater Than (Case Insensitive)",
                    "Less Than",
                    "Less Than (Case Insensitive)",
                    "Greater Than or Equal",
                    "Greater Than or Equal (Case Insensitive)",
                    "Less Than or Equal",
                    "Less Than or Equal (Case Insensitive)"
                };
            }
        }

        [PropertyHidden]
        [DataMember]
        public override string EntityName
        {
            get { return $"{Property} {Verb} {Value}"; }
            set { }
        }

        [PropertyHidden]
        [DataMember]
        public override string EntityDescription
        {
            get { return $"Filter: {Property} {Verb} {Value}"; }
            set { }
        }

        public PayloadFilter()
        {
            Id = IDUtility.GetNewIdString();
        }

        public PayloadFilter(string property, string verb, string value)
        {
            Id = IDUtility.GetNewIdString();
            Property = property;
            Verb = verb;
            Value = value;
        }

        public override BaseActionType[] GetActions(AbstractUserContext userContext, EntityActionType[] types)
        {
            return new BaseActionType[0];
        }
    }
}