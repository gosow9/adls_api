from pipeline.pipeline import Pipeline

def main():
    root_path = "."                # or wherever your YAML resides
    yaml_name = "example.yaml"     # name of the YAML file
    pipeline_name = "pipeline1"    # the pipeline you want to run
    
    # Create a Pipeline object
    pipe = Pipeline(root_path, yaml_name, pipeline_name)
    
    # Run tasks individually
    pipe.run_task("task1")  
    pipe.run_task("task2")  
    pipe.run_task("task3")
    
    # Or run tasks in a single invocation
    # pipe.run_task("task1,task2,task3")

if __name__ == "__main__":
    main()
