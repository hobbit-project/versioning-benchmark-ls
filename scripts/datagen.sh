mvn clean package -U -Dmaven.test.skip=true
docker build -f docker/versioningdatagenerator.docker -t git.project-hobbit.eu:4567/papv/versioningdatagenerator:1.0 . 
docker push git.project-hobbit.eu:4567/papv/versioningdatagenerator:1.0
