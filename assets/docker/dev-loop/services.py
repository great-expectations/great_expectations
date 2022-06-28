import shlex
from subprocess import Popen, run

jupyter_cmd = shlex.split("jupyter notebook great_expectations --ip=0.0.0.0 --port=5050 --no-browser")
datadocs_cmd = shlex.split("python -m http.server 5055 -d great_expectations/uncommitted/data_docs")

data_docs_proc = Popen(datadocs_cmd)
try:
    run(jupyter_cmd)
except KeyboardInterrupt:
    print("Shutting down GE Dev-Loop Container")
except Exception as e:
    print(f"Shutting down jupyter server due to exception {e}")
finally:
    data_docs_proc.terminate()
    print('GE Dev-Loop Container has exited')
