default: build dockerize-all

build:	
	mvn clean package -U -Dmaven.test.skip=true

dockerize-all: dockerize-controller dockerize-datagen dockerize-evalmodule
	
dockerize-controller:
	docker build -f docker/versioningbenchmarkcontroller.docker -t git.project-hobbit.eu:4567/papv/versioningbenchmarkcontroller:ls .
	docker push git.project-hobbit.eu:4567/papv/versioningbenchmarkcontroller:ls
		
dockerize-datagen:
	docker build -f docker/versioningdatagenerator.docker -t git.project-hobbit.eu:4567/papv/versioningdatagenerator:ls .
	docker push git.project-hobbit.eu:4567/papv/versioningdatagenerator:ls
	
dockerize-evalmodule:
	docker build -f docker/versioningevaluationmodule.docker -t git.project-hobbit.eu:4567/papv/versioningevaluationmodule:ls .
	docker push git.project-hobbit.eu:4567/papv/versioningevaluationmodule:ls