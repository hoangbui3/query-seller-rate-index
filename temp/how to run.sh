docker build -t seller-bulk-index .
docker run --name sellers-container -v "$(pwd)":/src -v "$(pwd)"/docker/supervisord/supervisor.d:/etc/supervisor.d seller-bulk-index
