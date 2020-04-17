#!/usr/bin/env python

import argparse
import boto3

# PARTITION

def copy_partition(original_table, destination_table):
    original_database = ""
    destination_database = ""

    if("." in original_table):
        original_database, original_table_name = original_table.split(".")
    if("." in destination_table):
        destination_database, destination_table_name = destination_table.split(".")

    client = boto3.client("glue")
    response = client.get_partitions(DatabaseName=original_database, TableName=original_table_name)
    partitions_input = response["Partitions"]

    # Delete keys that cause create_partition to fail
    for partition_input in partitions_input:
        partition_input.pop("CreationTime")
        partition_input.pop("DatabaseName")
        partition_input.pop("TableName")

    for i in range(0, len(partitions_input), 100):
        batch_partitions_input = partitions_input[i:i+100]
        client.batch_create_partition(DatabaseName=destination_database, TableName=destination_table_name, PartitionInputList=batch_partitions_input)

# TABLE

def copy_table(original_table, destination_table):
    original_database = ""
    destination_database = ""

    if("." in original_table):
        original_database, original_table_name = original_table.split(".")
    if("." in destination_table):
        destination_database, destination_table_name = destination_table.split(".")

    client = boto3.client("glue")
    response = client.get_table(DatabaseName=original_database, Name=original_table_name)
    table_input = response["Table"]
    table_input["Name"] = destination_table_name

    # Delete keys that cause create_table to fail
    table_input.pop("DatabaseName")
    table_input.pop("CreateTime")
    table_input.pop("UpdateTime")
    
    if("CreatedBy" in table_input):
        table_input.pop("CreatedBy")
    
    table_input.pop("IsRegisteredWithLakeFormation")

    client.create_table(DatabaseName=destination_database, TableInput=table_input)
    copy_partition(original_table, destination_table)

def delete_table(table):
    database = ""

    if("." in table):
        database, table_name = table.split(".")

    client = boto3.client("glue")
    client.delete_table(DatabaseName=database, Name=table_name)

def list_tables(original_database):
    client = boto3.client("glue")
    response = client.get_tables(DatabaseName=original_database)

    for table in response["TableList"]:
        print(table['Name'])

# DATABASE

def create_database(database):
    client = boto3.client("glue")
    client.create_database(DatabaseInput={ "Name": database })

if __name__ == '__main__':
    main_parser = argparse.ArgumentParser(description='Helper to AWS Glue')
    
    service_subparsers = main_parser.add_subparsers(title="service", dest="service_command")
    
    # TABLE ACTIONS
    table_parser = service_subparsers.add_parser("table", help="Service")

    table_action_subparser = table_parser.add_subparsers(title="action", dest="action_command")
    table_action_subparser.required = True

    table_action_copy_parser = table_action_subparser.add_parser("cp", help="Action")
    table_action_copy_parser.add_argument('table')
    table_action_copy_parser.add_argument('destination_table')

    table_action_list_parser = table_action_subparser.add_parser("ls", help="Action")
    table_action_list_parser.add_argument('database')

    table_action_delete_parser = table_action_subparser.add_parser("rm", help="Action")
    table_action_delete_parser.add_argument('table')

    # PARTITION ACTIONS
    partition_parser = service_subparsers.add_parser("partition", help="Service")

    partition_action_subparser = partition_parser.add_subparsers(title="action", dest="action_command")
    partition_action_subparser.required = True

    partition_action_copy_parser = partition_action_subparser.add_parser("cp", help="Action")
    partition_action_copy_parser.add_argument('table')
    partition_action_copy_parser.add_argument('destination_table')

    # DATABASE ACTIONS
    database_parser = service_subparsers.add_parser("database", help="Service")

    database_action_subparser = database_parser.add_subparsers(title="action", dest="action_command")
    database_action_subparser.required = True

    database_action_copy_parser = database_action_subparser.add_parser("create", help="Action")
    database_action_copy_parser.add_argument('database')

    args = main_parser.parse_args()

    if(args.service_command == 'table'):
        if(args.action_command == 'cp'):
            copy_table(args.table, args.destination_table)
        elif(args.action_command == 'ls'):
            list_tables(args.database)
        elif(args.action_command == 'rm'):
            delete_table(args.table)
    elif(args.service_command == 'partition'):
        if(args.action_command == 'cp'):
            copy_partition(args.table, args.destination_table)
    elif(args.service_command == 'database'):
        if(args.action_command == 'create'):
            create_database(args.database)