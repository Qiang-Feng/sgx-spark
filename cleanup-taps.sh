for i in {0..50}; do sudo ip tuntap del dev tap${i} mode tap; done;