import subprocess

from simpleflow.download import with_binaries


@with_binaries(
    {
        "how-is-simpleflow": "s3://botify-labs-simpleflow/binaries/latest/how_is_simpleflow",
    }
)
def a_task():
    print("command: which how-is-simpleflow")
    print(subprocess.check_output(["which", "how-is-simpleflow"]))

    print("command: how-is-simpleflow")
    print(subprocess.check_output(["how-is-simpleflow"]))


a_task()
