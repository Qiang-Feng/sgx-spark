ALPINE_MAJOR=3.9
ALPINE_VERSION=3.9.0
ALPINE_ARCH=x86_64

ALPINE_TAR=alpine-minirootfs.tar.gz

ROOT_FS=sgxlkl-java-fs.img
MOUNTPOINT=/media/ext4disk
IMAGE_SIZE_MB=128
SPARK_HOME=/home/qiang/Documents/sgx-spark

ESCALATE_CMD=sudo

.DELETE_ON_ERROR:
.PHONY: all clean

all: $(ROOT_FS)

clean:
	rm -rf $(ROOT_FS)

$(ALPINE_TAR):
	curl -L -o "$@" "https://nl.alpinelinux.org/alpine/v$(ALPINE_MAJOR)/releases/$(ALPINE_ARCH)/alpine-minirootfs-$(ALPINE_VERSION)-$(ALPINE_ARCH).tar.gz"

$(ROOT_FS): $(ALPINE_TAR) buildenv.sh
	dd if=/dev/zero of="$@" count=$(IMAGE_SIZE_MB) bs=1M
	mkfs.ext4 "$@"
	$(ESCALATE_CMD) /bin/bash -euxo pipefail -c '\
		mkdir -p $(MOUNTPOINT); \
		mount -t ext4 -o loop "$@" $(MOUNTPOINT); \
		tar -C $(MOUNTPOINT) -xvf $(ALPINE_TAR); \
		cp /etc/resolv.conf $(MOUNTPOINT)/etc/resolv.conf; \
		install buildenv.sh $(MOUNTPOINT)/usr/sbin; \
		mkdir -p $(MOUNTPOINT)/$(SPARK_HOME)/assembly/target/scala-2.11/jars; \
		cp ../assembly/target/scala-2.11/jars/spark-launcher_2.11-2.4.0-SGX.jar $(MOUNTPOINT)/$(SPARK_HOME)/assembly/target/scala-2.11/jars; \
		cp ../assembly/target/scala-2.11/jars/spark-core_2.11-2.4.0-SGX.jar $(MOUNTPOINT)/$(SPARK_HOME)/assembly/target/scala-2.11/jars; \
		cp ../assembly/target/scala-2.11/jars/scala-library-2.11.12.jar $(MOUNTPOINT)/$(SPARK_HOME)/assembly/target/scala-2.11/jars; \
		cp ../assembly/target/scala-2.11/jars/slf4j-api-1.7.16.jar $(MOUNTPOINT)/$(SPARK_HOME)/assembly/target/scala-2.11/jars; \
		cp ../assembly/target/scala-2.11/jars/slf4j-log4j12-1.7.16.jar $(MOUNTPOINT)/$(SPARK_HOME)/assembly/target/scala-2.11/jars; \
		cp ../assembly/target/scala-2.11/jars/log4j-1.2.17.jar $(MOUNTPOINT)/$(SPARK_HOME)/assembly/target/scala-2.11/jars; \
		cp ../assembly/target/scala-2.11/jars/hadoop-common-2.7.3.jar $(MOUNTPOINT)/$(SPARK_HOME)/assembly/target/scala-2.11/jars; \
		cp ../assembly/target/scala-2.11/jars/commons-lang-2.6.jar $(MOUNTPOINT)/$(SPARK_HOME)/assembly/target/scala-2.11/jars; \
		cp ../assembly/target/scala-2.11/jars/aircompressor-0.10.jar $(MOUNTPOINT)/$(SPARK_HOME)/assembly/target/scala-2.11/jars; \
		mkdir -p $(MOUNTPOINT)/$(SPARK_HOME)/conf; \
		cp ../conf/* $(MOUNTPOINT)/$(SPARK_HOME)/conf; \
		mkdir -p $(MOUNTPOINT)/opt; \
		chroot $(MOUNTPOINT) /bin/sh /usr/sbin/buildenv.sh; \
		du -hs $(MOUNTPOINT); \
		umount $(MOUNTPOINT); \
		chown $(USER) "$@"; \
	'
