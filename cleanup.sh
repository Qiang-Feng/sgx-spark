sudo killall sgx-lkl-java
sudo killall sgx-lkl-run
sudo killall ENCLAVE
sudo killall python3
sudo killall python

for i in {0..50}; do sudo ip tuntap del dev tap${i} mode tap; done;

find workers -type f -not -name 'base*' -delete
