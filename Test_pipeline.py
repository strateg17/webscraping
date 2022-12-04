"""
This file was designed as a copy of jupyter notebook from 
Azure ML worspace for configuration and running our pipeline.
"""

import os
import azureml.core
from azureml.core import Workspace
from azureml.core import Experiment
from azureml.core import Environment
from azureml.pipeline.core import Pipeline
from azureml.pipeline.core import PipelineData
from azureml.pipeline.steps import PythonScriptStep
from azureml.core.runconfig import RunConfiguration
from azureml.core.compute import ComputeTarget, AmlCompute
from azureml.core.compute_target import ComputeTargetException
from azureml.pipeline.core import ScheduleRecurrence, Schedule


################################################################################
# Create and config our workspace
################################################################################

# Manualy define workspace config
# ws = Workspace.get(
#            name="webparsing",
#            subscription_id= "1e70b6a9-7079-4703-a282-0c52141550cc",
#            resource_group= "webparsing"
# )

# Load the workspace from the saved config file
ws = Workspace.from_config()

print('Workspace name: ' + ws.name, 
      'Azure region: ' + ws.location, 
      'Subscription id: ' + ws.subscription_id, 
      'Resource group: ' + ws.resource_group, sep = '\n')




# Create a folder for the pipeline step files
experiment_folder = 'OlxParser_pipeline'
os.makedirs(experiment_folder, exist_ok=True)
print(experiment_folder)


# This block is needed to define Compute Target or create it, if we haven't it
cluster_name = "OLXParserCluster"

try:
    # Check for existing compute target
    pipeline_cluster = ComputeTarget(workspace=ws, name=cluster_name)
    print('Found existing cluster, use it.')
    print(f'Cluster name: {cluster_name}')
except ComputeTargetException:
    # If it doesn't already exist, create it
    try:
        compute_config = AmlCompute.provisioning_configuration(vm_size='STANDARD_DS11_V2', max_nodes=2)
        pipeline_cluster = ComputeTarget.create(ws, cluster_name, compute_config)
        pipeline_cluster.wait_for_completion(show_output=True)
    except Exception as ex:
        print(ex)


# Create an Environment
pipeline_env = Environment("OLX_Parsing")


# Set the container registry information for private containers
# pipeline_env.docker.base_image_registry.address = "webparsingcontainer.azurecr.io"
# pipeline_env.docker.base_image_registry.username = "WebParsing"
# pipeline_env.docker.base_image_registry.password = "OvrOqNkqD5V8/EmeJcgBLbhMpYXvIX5S"
# pipeline_env.docker.base_image = "webparsingcontainer.azurecr.io/web_parsing:v2"
# pipeline_env.python.user_managed_dependencies = True


# When you're using your custom Docker image, you might already have your Python environment properly set up. 
# In that case, set the user_managed_dependencies flag to True to use your custom image's built-in Python environment.
pipeline_env.docker.base_image = "strateg17/web_parsing:v2"
pipeline_env.python.user_managed_dependencies = True


# Register the environment 
pipeline_env.register(workspace=ws)
registered_env = Environment.get(ws, 'OLX_Parsing')

# Create a new runconfig object for the pipeline
pipeline_run_config = RunConfiguration()

# Use the compute you created above. 
pipeline_run_config.target = pipeline_cluster

# Assign the environment to the run configuration
pipeline_run_config.environment = registered_env
print ("Run configuration created.")


# from azureml.core.datastore import Datastore
# pages_datastore = Datastore.get(
#     workspace=ws, 
#     datastore_name="olx_pages_datastore"
# )



# Difine Pipeline steps
# input and output data parameters for each step  
pages_param = PipelineData('pages',  is_directory=True)
images_param = PipelineData('images', is_directory=True)


write_output_step = PythonScriptStep(
    name='The Page Saver',
    script_name='Step_1_OlxParser_azure.py', # sript with 1st step 
    arguments = ["--pages-dir", pages_param],
    outputs=[pages_param], # output directory defined as arguments
    compute_target=pipeline_cluster,
    source_directory='OlxParser_pipeline', # folder with script
    runconfig = pipeline_run_config, # env config
    allow_reuse=False
)


read_output_step = PythonScriptStep(
     name='The Page Parser',
    script_name='Step_2_OlxParser_azure.py', # sript with 2nd step 
    arguments = ["--pages-dir", pages_param, "--images-dir", images_param],
    inputs = [pages_param], # input directory defined as arguments
    outputs=[images_param], # output directory defined as arguments
    compute_target=pipeline_cluster,
    source_directory='OlxParser_pipeline', # folder with script
    runconfig = pipeline_run_config, # env config
    allow_reuse=False
)




# Construct the pipeline
pipeline_steps = [write_output_step, read_output_step]
pipeline = Pipeline(workspace=ws, steps=pipeline_steps)
print("Pipeline is built.")

# Create an experiment and run the pipeline
experiment = Experiment(workspace=ws, name = 'OLX-parsing-pipeline')
pipeline_run = experiment.submit(pipeline, regenerate_outputs=True)
print("Pipeline submitted for execution.")

# Run the pipeline
pipeline_run.wait_for_completion(show_output=True)


# Publish the pipeline from the run
published_pipeline = pipeline_run.publish_pipeline(
    name="olx-parsing-pipeline", description="Retrive info from OLX", version="1.0")

print(published_pipeline)


# Submit the Pipeline every Day of the week at 00:00 UTC
from azureml.pipeline.core import ScheduleRecurrence, Schedule, TimeZone

# Submit the Pipeline every Day of the week at 00:00 UTC
recurrence = ScheduleRecurrence(frequency="Day", 
                                interval=1, 
                                time_of_day="01:00",
                                time_zone=TimeZone.UTC02)

daily_schedule = Schedule.create(ws, name="daily-olx-parser", 
                                  description="Based on time",
                                  pipeline_id=published_pipeline.id, 
                                  experiment_name='OLX-parsing-pipeline', 
                                  recurrence=recurrence)
print('Pipeline scheduled.')