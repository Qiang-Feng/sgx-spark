import subprocess
import os
import sys
import struct
import random
import pytun

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
    "objenesis-2.5.1.jar",
    "kryo-shaded-4.0.2.jar",
    "log4j-1.2.17.jar",
    "chill_2.11-0.9.3.jar",
    "chill-java-0.9.3.jar",
    "minlog-1.3.0.jar",
    "RoaringBitmap-0.5.11.jar",
    "avro-1.8.2.jar"
]

def daemon():
    stdin_bin = os.fdopen(sys.stdin.fileno(), "rb", 4)
    stdout_bin = os.fdopen(sys.stdout.fileno(), "wb", 4)

    try:
        while True:
            # Block until we get a factory port
            factory_port = binary_to_int(stdin_bin.read(4))

            # Set factory port in worker env
            ENV["SGX_WORKER_FACTORY_PORT"] = str(factory_port)

            # Generate random local IP
            ip_base = "10.{}.{}".format(*random.sample(range(0, 255), 2))
            ip_worker = "{}.1".format(ip_base)
            ip_host = "{}.2".format(ip_base)
            print("Base TAP IP: {}".format(ip_base), file=sys.stderr)

            # Create TAP device
            tap = pytun.TunTapDevice(flags=pytun.IFF_TAP)
            tap.addr = ip_host
            tap.dstaddr = ip_worker
            tap.netmask = '255.255.255.255'
            tap.up()
            tap.persist(True)
            tap.close()

            # TODO: Delete the tap device when we the SGX Worker shuts down

            # Launch the SGX Worker process using the newly created image
            worker_command = 'SGXLKL_VERBOSE=1 ' \
                             'SGXLKL_HD_KEY={}/sgx-worker.img.enc.int.key ' \
                             'SGXLKL_TAP={} ' \
                             'SGXLKL_IP4={} ' \
                             'SGXLKL_GW4={} ' \
                             '/usr/local/build/sgx-lkl-java ' \
                             '{}/sgx-worker.img ' \
                             '-cp "{}" ' \
                             'org.apache.spark.deploy.worker.sgx.SGXWorker'.format(tap.name, ip_worker, ip_host, SPARK_HOME, ":".join(DEFAULT_CLASSPATH))
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

            # TODO: Delete SGX Worker images once it has exited

    except IOError:
        print("Shutting down daemon.", file=sys.stderr)
        sys.exit(0)

def binary_to_int(b):
    return struct.unpack("!i", b)[0]

def int_to_binary(i):
    return struct.pack("!i", i)

if __name__ == '__main__':
    daemon()
