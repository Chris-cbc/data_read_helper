from base.runner import Executor

if __name__ == "__main__":
    yml_file = "./workplace/app/demo.yml"
    executor = Executor(yml_file)
    df = executor.process_table()
    print(df)
