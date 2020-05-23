import subprocess
import os
import sys
import struct
import tempfile
import uuid

SPARK_HOME = os.getenv("SPARK_HOME", ".")
SGX_WORKER_MODULE = "org.apache.spark.deploy.worker.sgx.SGXWorker"
SGX_WORKER_EXEC = SPARK_HOME + "/sbin/start-sgx-slave.sh"
ENV = os.environ.copy()
END_OF_DATA_SECTION = -1
DEFAULT_CLASSPATH = [
    "spark-unsafe_2.11-2.4.0-SGX.jar",
    "hadoop-mapreduce-client-core-2.7.3.jar",
    "scala-library-2.11.12.jar",
    "spark-network-common_2.11-2.4.0-SGX.jar",
    "spark-core_2.11-2.4.0-SGX.jar",
    "slf4j-api-1.7.16.jar",
    "hadoop-common-2.7.3.jar",
    "commons-lang3-3.5.jar",
    "slf4j-log4j12-1.7.16.jar",
    "log4j-1.2.17.jar"
]

def daemon():
    # Load Dockerfile template
    df_template = ""
    with open("Dockerfile.template", "r") as df:
        df_template += df.read()

    stdin_bin = os.fdopen(sys.stdin.fileno(), "rb", 4)
    stdout_bin = os.fdopen(sys.stdout.fileno(), "wb", 4)

    try:
        while True:
            # Block until we get a factory port
            factory_port = binary_to_int(stdin_bin.read(4))

            # Set factory port in worker env
            ENV["SGX_WORKER_FACTORY_PORT"] = str(factory_port)

            # Create temporary Dockerfile with newJars for new SGXWorker image
            classpath = DEFAULT_CLASSPATH
            worker_img_name = uuid.uuid4()
            with tempfile.NamedTemporaryFile(mode="w+", dir=SPARK_HOME) as df:
                df.write(df_template)

                # Read new jars
                path_length = binary_to_int(stdin_bin.read(4))
                while path_length != END_OF_DATA_SECTION:
                    full_path = stdin_bin.read(path_length).decode("utf-8").replace(SPARK_HOME, "")
                    jar_name = os.path.basename(full_path)
                    classpath.append(jar_name)

                    df.write("COPY {} {}\n".format(full_path, jar_name))
                    path_length = binary_to_int(stdin_bin.read(4))

                df.flush()

                # Build docker image with new Dockerfile
                builder_command = "yes | " \
                                  "/usr/local/build/sgx-lkl-disk " \
                                  "create " \
                                  "-V " \
                                  "--size=512M " \
                                  "--docker={} " \
                                  "{}/{}.img".format(df.name, SPARK_HOME, worker_img_name)
                print("Running: {}".format(builder_command), file=sys.stderr)
                builder = subprocess.Popen(
                    [builder_command],
                    shell=True,
                    stdout=sys.stderr,
                    stderr=sys.stderr,
                    env=ENV
                )
                builder.wait()

            # Launch the SGX Worker process using the newly created image
            worker_command = 'SGXLKL_VERBOSE=1 ' \
                            'SGXLKL_TAP=sgxlkl_tap0 ' \
                            '/usr/local/build/sgx-lkl-java ' \
                            '{}/{}.img ' \
                            '-cp "{}" ' \
                            'org.apache.spark.deploy.worker.sgx.SGXWorker'.format(SPARK_HOME, worker_img_name, ":".join(classpath))
            print("Running {}".format(worker_command), file=sys.stderr)
            worker = subprocess.Popen(
                [worker_command],
                shell=True,
                stdout=sys.stderr,
                stderr=sys.stderr,
                env=ENV
            )

            print("Created worker with PID {}".format(worker.pid), file=sys.stderr)
            stdout_bin.write(int_to_binary(worker.pid))
            stdout_bin.flush()

    except IOError:
        print("Shutting down daemon.", file=sys.stderr)
        sys.exit(0)

def binary_to_int(b):
    return struct.unpack("!i", b)[0]

def int_to_binary(i):
    return struct.pack("!i", i)

if __name__ == '__main__':
    daemon()
