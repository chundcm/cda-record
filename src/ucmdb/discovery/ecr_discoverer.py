#coding=utf-8
import logger
import modeling
from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector

import ecs

def discoverEcrTopology(framework, service, account, credential_id, resourceDict):
    class EcrDiscoverer:
        def __init__(self, service, account, region):
            self._service = service
            self._account = account
            self._region = region
            self.Images = []
            self.Repositories = {}
            self.Registry = None

        def buildEcrTopology(self):
            for key,container in resourceDict['EcsContainers'].items():
                logger.debug('container: ', key)
                logger.debug('container.imageName: ', container.imageName)
            self.getRepos()
            self.getImages()
            return self.reportEcr()

        def getImages(self):
            from com.amazonaws.services.ecr.model import DescribeImagesRequest
            try:
                for key, container in resourceDict['EcsContainers'].items():
                    logger.info('container.imageName: ', container.imageName)
                    if container.imageName.find(':') != -1:
                        imageRepo = container.imageName.split(':')[0]
                        imageTag = container.imageName.split(':')[1]
                    else:
                        imageRepo = container.imageName
                        imageTag = 'latest'
                    if self.Repositories.has_key(imageRepo):
                        repoArn = self.Repositories[imageRepo].name
                        request = DescribeImagesRequest().withRepositoryName(repoArn)
                        imageDetail = self._service.describeImages(request).getImageDetails()[0]
                        image = buildImage(container, imageDetail, imageTag, account, self.Repositories[imageRepo])
                        self.Images.append(image)
            except:
                logger.warnException('Fail to get images!')

        def getRepos(self):
            from com.amazonaws.services.ecr.model import DescribeRepositoriesRequest
            registryId = self._account.getId()
            reg = buildPrivateRegistry(registryId, self._region)
            self.Registry = reg
            try:
                request = DescribeRepositoriesRequest().withRegistryId(registryId)
                repositories = self._service.describeRepositories(request).getRepositories()
                for repositoryItem in repositories:
                    repository = buildRepository(reg, repositoryItem)
                    self.Repositories[repository.description] = repository
            except:
                logger.warnException('Fail to get repositories!')

        def reportEcr(self):
            vector = ObjectStateHolderVector()
            if self.Registry:
                accountLink = modeling.createLinkOSH('containment', self._account.getOsh(), self.Registry.getOsh())
                vector.add(accountLink)
                regionLink = modeling.createLinkOSH('membership', self._region.getOsh(), self.Registry.getOsh())
                vector.add(regionLink)
                vector.add(self.Registry.getOsh())
            for _, repository in self.Repositories.items():
                vector.add(repository.getOsh())
                repositoryOSH = repository.getOsh()
                repositoryOSH.setContainer(repository.Registry.getOsh())
            # report image
            for image in self.Images:
                imageOSH = image.getOsh()
                vector.add(imageOSH)
                imageOSH.setContainer(image.Ec2Instance)
                imageContainerLink = modeling.createLinkOSH('realization', imageOSH, image.Container.getOsh())
                vector.add(imageContainerLink)

                # report image template
                templateOSH = ObjectStateHolder('docker_image_template')
                templateOSH.setAttribute('name', image.imageId)
                templateOSH.setAttribute('docker_image_id', image.imageId)
                vector.add(templateOSH)

                # report image tag if the image belongs to private registry
                if image.fromPrivateRegistry:
                    imageTemplateLink = modeling.createLinkOSH('resource', templateOSH, image.getOsh())
                    vector.add(imageTemplateLink)

                    # link to repository
                    if image.Repository:
                        tagOSH = ObjectStateHolder('docker_image_tag')
                        tagOSH.setAttribute('name', image.imageTag)
                        vector.add(tagOSH)
                        repositoryOSH = image.Repository.getOsh()
                        tagOSH.setContainer(repositoryOSH)
                        tagRepositoryLink = modeling.createLinkOSH('composition', repositoryOSH, tagOSH)
                        vector.add(tagRepositoryLink)
                        imageRepositoryLink = modeling.createLinkOSH('composition', repositoryOSH, image.getOsh())
                        vector.add(imageRepositoryLink)
                        tagTemplateLink = modeling.createLinkOSH('resource', templateOSH, tagOSH)
                        vector.add(tagTemplateLink)
            return vector

    logger.info('ECR TOPOLOGY DISCOVERY')
    vector = ObjectStateHolderVector()
    if not resourceDict['Regions']:
        raise Exception('No region found for ECR discovery.')
    for region in resourceDict['Regions']:
        try:
            service.setEndpoint(region.getEndpointHostName().replace('ec2', 'ecr'))
            ecrDiscoverer = EcrDiscoverer(service, account, region)
            vector.addAll(ecrDiscoverer.buildEcrTopology())
        except:
            logger.warnException('Fail in region:', region)
    return vector


def buildPrivateRegistry(accountId, region):
    reg = ecs.DockerRegistry(accountId, region)
    reg.setArn(reg.name)
    osh = ObjectStateHolder('docker_registry_system')
    osh.setAttribute('name', reg.name)
    reg.setOsh(osh)
    logger.debug('Discovered : ', reg)
    return reg


def buildImage(container, imageDetail, imageTag, account, repo):
    image = ecs.DockerImage()
    image.imageId = imageDetail.getImageDigest()
    image.repository = imageDetail.getRepositoryName()
    image.imageTag = imageTag
    image.name = image.repository + ':' + image.imageTag
    image.Repository = repo
    image.setArn(image.name)
    if account.getId() == str(imageDetail.getRegistryId()):
        image.fromPrivateRegistry = True
    image.Container = container
    image.Ec2Instance = container.Ec2Instance
    osh = ObjectStateHolder('docker_image')
    osh.setAttribute('name', image.name)
    osh.setAttribute('docker_image_id', image.imageId)
    osh.setAttribute('repository', image.repository)
    osh.setAttribute('tag', image.imageTag)
    image.setOsh(osh)
    logger.debug('Discovered : ', image)
    return image


def buildRepository(registry, imageDetail):
    repository = ecs.DockerRepository(imageDetail.getRepositoryName())
    repository.description = imageDetail.getRepositoryUri()
    repository.Registry = registry
    repository.setArn(repository.name)
    osh = ObjectStateHolder('docker_repository')
    osh.setAttribute('name', repository.name)
    osh.setAttribute('description', repository.description)
    repository.setOsh(osh)
    logger.debug('Discovered : ', repository)
    return repository

