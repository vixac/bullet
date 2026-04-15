docker pull postgres

docker run --name devdb -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d postgres
