mkdir -p workers
sgx-lkl-disk create -V --size=256M --docker=Dockerfile workers/base.img
