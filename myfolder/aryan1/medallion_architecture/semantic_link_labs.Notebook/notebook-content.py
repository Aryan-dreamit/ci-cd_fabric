# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c82cf3dd-7102-40d5-b484-0d0db5470c20",
# META       "default_lakehouse_name": "gold_layer",
# META       "default_lakehouse_workspace_id": "2fe9abb6-9a56-4088-8062-472540353376",
# META       "known_lakehouses": [
# META         {
# META           "id": "c82cf3dd-7102-40d5-b484-0d0db5470c20"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************


%pip install semantic-link-labs



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **Import the Library and set initial parameters**

# CELL ********************

import sempy_labs as labs 
from sempy_labs import migration, directlake 
import sempy_labs.report as rep 
dataset_name="gold_semantic"
workspace_name="Test_ServicePrincipal"
new_dataset_name="gold_semantic_directlake"
new_dataset_workspace_name="Test_ServicePrincipal"
source_dataset_id="762436eb-900d-4dd4-ae6f-98e4f7785a40"
target_dataset_id="ae3da5cb-21e4-4b95-a5da-8eda618b10ee"
table_name="billing_customer",
# labs.refresh_dataset(dataset_name=new_dataset_name, workspace_name=new_dataset_workspace_name)

# directlake.save_dataset(dataset_name=new_dataset_name, workspace_name=new_dataset_workspace_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

report_name="test1_dashboard (1)"
rep.report_rebind(
    report=report_name,
    dataset=new_dataset_name,
    report_workspace=workspace_name,
    dataset_workspace=new_dataset_workspace_name,
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sempy_labs as labs
import sempy.fabric as fabric
import time

def migrate_semantic_model(source_workspace_id, source_dataset_id, target_workspace_id, target_dataset_name):
    """
    Sandeep Pawar | fabric.guru | 02-10-2025
    Migrates/copies a semantic model from a source workspace to a target workspace.

    Parameters:
        source_workspace_id (str): The ID of the source workspace.
        source_dataset_id (str): The ID of the source dataset.
        target_workspace_id (str): The ID of the target workspace.
        target_dataset_name (str): The name of the target dataset.
    """
    # current bim
    bim_file = labs.get_semantic_model_bim(dataset=source_dataset_id, workspace=source_workspace_id)

    # see if the model with the same name exists
    datasets = fabric.list_datasets(workspace=target_workspace_id)
    if not any(datasets['Dataset Name'].isin([target_dataset_name])):
        # first a blank model
        labs.create_blank_semantic_model(dataset=target_dataset_name, workspace=target_workspace_id)
        # update blank with above bim
        labs.update_semantic_model_from_bim(dataset=target_dataset_name, bim_file=bim_file, workspace=target_workspace_id)
        print(f"Semantic model '{target_dataset_name}' has been created and updated in the target workspace.")

    else:
        print(f"Semantic model with the name '{target_dataset_name}' already exists in the target workspace.")
        datasets.query('`Dataset Name` == @target_dataset_name')
        display(datasets.query('`Dataset Name` == @target_dataset_name'))


source_workspace_id = "2fe9abb6-9a56-4088-8062-472540353376"
source_dataset_id = "762436eb-900d-4dd4-ae6f-98e4f7785a40"
target_workspace_id = "2fe9abb6-9a56-4088-8062-472540353376"
target_dataset_name = "gold_semantic_directlake"

migrate_semantic_model(source_workspace_id, source_dataset_id, target_workspace_id, target_dataset_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# rep.report_rebind_all(
#     dataset=dataset_name,
#     dataset_workspace=workspace_name,
#     new_dataset=new_dataset_name,
#     new_dataset_workspace=new_dataset_workspace_name,
#     report_workspace=workspace_name,
    
# )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# dfR = fabric.list_reports(workspace=workspace_name)
# print(dfR.columns)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
