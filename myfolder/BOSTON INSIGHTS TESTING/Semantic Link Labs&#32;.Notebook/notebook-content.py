# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

! pip install semantic-link-labs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # LIBRARIES

# CELL ********************

import sempy_labs as labs
import sempy.fabric as fabric
import sempy
from sempy_labs import migration, directlake, graph
from sempy_labs import lakehouse as lake
from sempy_labs import report as report
import sempy_labs.tom as tom
from sempy_labs.report import ReportWrapper

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Listing Semantic Models Present In Specific workspace

# CELL ********************

selectedWorkspace = "2fe9abb6-9a56-4088-8062-472540353376" 
itemType = "SemanticModel"

df = fabric.list_items(workspace=selectedWorkspace, type=itemType)
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Filtering specific Semantic model from multiples

# CELL ********************

# df_clean = df[df['Display Name'].str.contains("test1_dashboard")]
df_source = df[df['Display Name']=="Suncare Walmart - Canada (53)"]
df_target = df[df['Display Name']=="SEMANTIC LINK LABS"]
print("Source Semantic model detail:")
display(df_source)
print("Target Semantic model detail:")
display(df_target)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_source['Id'].iloc[0])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# labs.run_model_bpa(df_source['Id'].iloc[0])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_workspace = df_source["Workspace Id"].iloc[0]
target_workspace = df_target["Workspace Id"].iloc[0]
source_dataset_name = df_source["Display Name"].iloc[0]
target_dataset_name = df_target["Display Name"].iloc[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

with tom.connect_semantic_model(source_dataset_name) as source_tom, \
     tom.connect_semantic_model(target_dataset_name) as target_tom:

    source_model = source_tom.model
    target_model = target_tom.model

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

    source_tables = source_model.Tables
    print("SOURCE TABLES:", [t.Name for t in source_tables])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sempy_labs.tom as tom
dir(tom)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

with tom.connect_semantic_model(target_dataset_name) as target_tom:

    print("\nADDING TABLES TO TARGET:")

    for table in source_tables:

        # Skip if table exists
        existing_tables = [t.Name for t in target_tom.model.Tables]
        if table.Name in existing_tables:
            print(f"Table already exists: {table.Name}")
            continue

        print(f"Creating table: {table.Name}")
        # Add table
        target_tom.add_table(table.Name, table.Description)

        # Get the table object from the model
        new_table = next(t for t in target_tom.model.Tables if t.Name == table.Name)

        # Add columns
        for col in table.Columns:
            print(f"   - Adding column: {col.Name}")
            data_type_str = str(col.DataType).split('.')[-1]  
            target_tom.add_data_column(
                table_name=new_table.Name,
                column_name=col.Name,
                source_column=None,
                data_type=data_type_str
            )
    # **Persist all changes**
    try:
        target_tom.model.SaveChanges()
        print("\nTABLES COPIED SUCCESSFULLY AND SAVED!")
    except Exception as e:
        print("Failed to save changes:", e)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Relationships
relationships = model.Relationships
print("RELATIONSHIPS:", [r.Name for r in relationships])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

with tom.connect_semantic_model(target_dataset_name) as target_tom:
    print("\nCOPYING RELATIONSHIPS TO TARGET:")

    # Build a map of target table names for easy lookup
    target_tables = {t.Name: t for t in target_tom.model.Tables}

    for rel in model.Relationships:
        from_table = rel.FromTable.Name
        from_col = rel.FromColumn.Name
        to_table = rel.ToTable.Name
        to_col = rel.ToColumn.Name

        # Skip if either table does not exist in target
        if from_table not in target_tables:
            print(f"Skipping relationship: From table '{from_table}' does not exist in target")
            continue
        if to_table not in target_tables:
            print(f"Skipping relationship: To table '{to_table}' does not exist in target")
            continue

        # Set cardinalities and directions as strings
        from_cardinality = "Many"
        to_cardinality = "One"
        cross_filtering_behavior = "Both" if str(rel.CrossFilteringBehavior).lower() == "both" else "Single"
        security_filtering_behavior = "OneDirection"

        # Skip if already exists
        existing_rels = [
            (r.FromTable.Name, r.FromColumn.Name, r.ToTable.Name, r.ToColumn.Name)
            for r in target_tom.model.Relationships
        ]
        if (from_table, from_col, to_table, to_col) in existing_rels:
            print(f"Relationship already exists: {from_table}.{from_col} → {to_table}.{to_col}")
            continue

        print(f"Creating relationship: {from_table}.{from_col} → {to_table}.{to_col}")

        target_tom.add_relationship(
            from_table,
            from_col,
            to_table,
            to_col,
            from_cardinality,
            to_cardinality,
            cross_filtering_behavior,
            rel.IsActive,
            security_filtering_behavior
        )

print("\nRELATIONSHIPS COPIED SUCCESSFULLY!")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Measures
measures = []
for t in model.Tables:
    for m in t.Measures:
        measures.append(m.Name)

print("MEASURES:", measures)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

with tom.connect_semantic_model(target_dataset_name) as target_tom:
    print("\nCOPYING MEASURES TO TARGET:")

    # Build a map of target tables for easier lookup
    target_tables = {t.Name: t for t in target_tom.model.Tables}

    for table in model.Tables:
        table_name = table.Name

        # Skip if table does not exist in target
        if table_name not in target_tables:
            print(f"Skipping measures for table '{table_name}' as it does not exist in target")
            continue

        target_table = target_tables[table_name]

        for measure in table.Measures:
            measure_name = measure.Name
            # Skip if measure already exists
            existing_measures = [m.Name for m in target_table.Measures]
            if measure_name in existing_measures:
                print(f"Measure already exists: {table_name}.{measure_name}")
                continue

            print(f"Adding measure: {table_name}.{measure_name}")

            # Add measure to target table
            target_tom.add_measure(
                table_name=table_name,
                measure_name=measure_name,
                expression=measure.Expression,
                format_string=measure.FormatString if hasattr(measure, "FormatString") else None,
                description=measure.Description if hasattr(measure, "Description") else None
            )

print("\nMEASURES COPIED SUCCESSFULLY!")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#!pip install semantic-link-labs --q
import sempy_labs as labs
import sempy.fabric as fabric
import time

def migrate_semantic_model(source_workspace_id, source_dataset_id, target_workspace_id, target_dataset_name):
    """
    Migrates/copies a semantic model from a source workspace to a target workspace.

    Parameters:
        source_workspace_id (str): The ID of the source workspace.
        source_dataset_id (str): The ID of the source dataset (semantic model ID).
        target_workspace_id (str): The ID of the target workspace.
        target_dataset_name (str): The name of the target dataset.
    """
    # Get BIM file from the source semantic model
    bim_file = labs.get_semantic_model_bim(dataset=source_dataset_id, workspace=source_workspace_id)

    # List existing datasets in the target workspace
    datasets = fabric.list_datasets(workspace=target_workspace_id)

    if not any(datasets['Dataset Name'].isin([target_dataset_name])):
        # Target dataset does not exist → create blank model first
        labs.create_blank_semantic_model(dataset=target_dataset_name, workspace=target_workspace_id)
        print(f"Created blank semantic model: '{target_dataset_name}' in target workspace.")

    else:
        print(f"Target semantic model '{target_dataset_name}' already exists. It will be updated.")

    # Update the target semantic model using the source BIM
    try:
        labs.update_semantic_model_from_bim(
            dataset=target_dataset_name,
            bim_file=bim_file,
            workspace=target_workspace_id
        )
        print(f"Semantic model '{target_dataset_name}' has been updated with source data.")
    except Exception as e:
        print("Failed to update target semantic model:", e)


# Example usage
source_workspace_id = "2fe9abb6-9a56-4088-8062-472540353376"  # source workspace
source_dataset_id   = "f95fb239-8b9e-44b9-bfd7-69d312c4c1cb"  # source semantic model ID
target_workspace_id = "2fe9abb6-9a56-4088-8062-472540353376"  # target workspace
target_dataset_name = "SEMANTIC LINK LABS"                     # target semantic model name

# Run migration
migrate_semantic_model(source_workspace_id, source_dataset_id, target_workspace_id, target_dataset_name)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
