mkdir -p workers
sgx-lkl-disk create -V --encrypt --key-file --integrity --size=256M --docker=Dockerfile workers/base.img.enc.int
