"""
Infrastructure-as-code script for managing AWS Redshift cluster creation and teardown
"""
import pandas as pd
import boto3
import json
import configparser
import os
import argparse
import time


########################################################################################
# Load DWH Params
########################################################################################
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY = os.getenv("AWS_ACCESS_KEY_ID")
SECRET = os.getenv("AWS_SECRET_ACCESS_KEY")

DWH_CLUSTER_TYPE       = config.get("DWH_SETUP","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH_SETUP","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH_SETUP","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH_SETUP","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("CLUSTER","DB_NAME")
DWH_DB_USER            = config.get("CLUSTER","DB_USER")
DWH_DB_PASSWORD        = config.get("CLUSTER","DB_PASSWORD")
DWH_PORT               = config.get("CLUSTER","DB_PORT")

DWH_IAM_ROLE_NAME      = config.get("IAM_ROLE", "DWH_IAM_ROLE_NAME")

########################################################################################
# Functions
########################################################################################

def create_iam_role(iam):
    """Create IAM role for managing the redshift cluster. Returns the role ARN"""
    try:
        print("1.1 Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )    
    except Exception as e:
        print(e)
        
        
    print("1.2 Attaching Policy")

    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                        )['ResponseMetadata']['HTTPStatusCode']

    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    print('Role ARN: ', roleArn)

    return roleArn


def create_redshfit_cluster(redshift, roleArn):
    """ Create Redshift cluster """
    try:
        response = redshift.create_cluster(        
            #HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            
            #Roles (for s3 access)
            IamRoles=[roleArn]  
        )
    except Exception as e:
        print(e)


def open_tcp(vpc_id):
    """ Open TCP connection """
    ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )
    try:
        vpc = ec2.Vpc(id=vpc_id)
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)

def delete_redshift_cluster(redshift):
    """ Delete the Redshift cluster """
    try: 
        redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
        print(f'Deleted cluster {DWH_CLUSTER_IDENTIFIER}')
    except Exception as e:
        print(e)

def delete_iam_role(iam):
    """ Delete IAM role """
    try: 
        iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
        print(f'Deleted role {DWH_IAM_ROLE_NAME}')
    except Exception as e:
        print(e)


 

########################################################################################
# Main
########################################################################################
def main(args):
    """Main function"""
    redshift = boto3.client('redshift',
                    region_name="us-west-2",
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET
                    )    
    iam = boto3.client('iam',aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET,
                        region_name='us-west-2'
                    )    
    if args.create:
        roleArn = create_iam_role(iam)

        create_redshfit_cluster(redshift, roleArn)

        sleep_for = 15 # Sleep for 15 seconds before entering each loop
        for _ in range(10):
            time.sleep(sleep_for)

            cluster = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

            if cluster['ClusterStatus'] == 'available':
                break
        
            print('Cluster status is "{}". Retrying in {} seconds.'.format(cluster['ClusterStatus'], sleep_for))

        # Open TCP connection upon successful cluster creation
        if cluster and cluster['ClusterStatus'] == 'available':
            print('Cluster created at {}'.format(cluster['Endpoint']))
            open_tcp(cluster['VpcId'])

        else:
            print('Could not connect to cluster')


    elif hasattr(args, 'delete') and args.delete:
        delete_redshift_cluster(redshift)
        delete_iam_role(iam)
        print('Successfully deleted cluster')
    
    else:
        print('Please pass either --delete or --create flags')


if __name__ == '__main__':
    """ Parse cli arguments """
    parser = argparse.ArgumentParser()
    parser.add_argument('--delete', dest='delete', default=False, action='store_true')
    parser.add_argument('--create', dest='create', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
