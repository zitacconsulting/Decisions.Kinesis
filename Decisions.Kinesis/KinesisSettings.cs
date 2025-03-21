using DecisionsFramework;
using DecisionsFramework.Data.ORMapper;
using DecisionsFramework.Design.ConfigurationStorage.Attributes;
using DecisionsFramework.Design.Properties;
using DecisionsFramework.Design.Properties.Attributes;
using DecisionsFramework.ServiceLayer.Utilities;
using System.Runtime.Serialization;
using Decisions.MessageQueues;

namespace Decisions.KinesisMessageQueue
{
    [AutoRegisterNativeType]
    [DataContract]
    public class KinesisSettings : BaseMqSettings<KinesisMessageQueue>
    {
        public override string LogCategory => "Kinesis";
        protected override string AddQueueActionText => "Add Kinesis Stream";
        protected override string QueueTypeName => "Kinesis";
        public override string ModuleName => "Decisions.Kinesis";
        public override bool IsExportable() => true;

        [ORMField]
        [WritableValue]
        private string awsRegion;

        [DataMember]
        [PropertyClassification(1, "AWS Region", "Settings")]
        [SelectStringEditor("RegionOptions")]
        public string AwsRegion
        {
            get { return awsRegion; }
            set { awsRegion = value; }
        }

        [ORMField]
        [WritableValue]
        private string authenticationMethod;

        [DataMember]
        [PropertyClassification(2, "Authentication Method", "Settings")]
        [SelectStringEditor("AuthenticationMethods")]
        public string AuthenticationMethod
        {
            get { return authenticationMethod; }
            set
            {
                authenticationMethod = value;
                OnPropertyChanged();
            }
        }

        [ORMField]
        [PropertyClassification(3, "Access Key ID", "Settings")]
        [WritableValue]
        [PropertyHiddenByValue(nameof(AuthenticationMethod), "StaticCredentials", false)]
        public string AccessKeyId { get; set; }

        [ORMField(4000, typeof(FixedLengthStringFieldConverter))]
        [PropertyClassification(4, "Secret Access Key", "Settings")]
        [PasswordText]
        [WritableValue]
        [PropertyHiddenByValue(nameof(AuthenticationMethod), "StaticCredentials", false)]
        public string SecretAccessKey { get; set; }

        [ORMField]
        [WritableValue]
        private bool useRoleArn;

        [DataMember]
        [PropertyClassification(4, "Assume Role", "Settings")]
        public bool UseRoleArn
        {
            get { return useRoleArn; }
            set
            {
                useRoleArn = value;
                OnPropertyChanged();
            }
        }

        [ORMField]
        [WritableValue]
        [DataMember]
        [PropertyClassification(5, "Role ARN", "Settings")]
        [BooleanPropertyHidden(nameof(UseRoleArn), false)]
        public string RoleArn { get; set; }




        [PropertyHidden]
        public string[] RegionOptions
        {
            get
            {
                return new string[]
                {
                    "us-east-1", "us-east-2", "us-west-1", "us-west-2",
                    "eu-west-1", "eu-west-2", "eu-central-1",
                    "ap-southeast-1", "ap-southeast-2", "ap-northeast-1", "ap-northeast-2",
                    "sa-east-1"
                };
            }
        }

        [PropertyHidden]
        public string[] AuthenticationMethods
        {
            get
            {
                return new string[]
                {
                    "DefaultCredentials",
                    "StaticCredentials"
                };
            }
        }
        
    }
}