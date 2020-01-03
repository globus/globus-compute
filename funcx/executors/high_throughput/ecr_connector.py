
import boto3

ecr_cli = boto3.client('ecr')


# TODO: Make sure the repo name is not unique.
response = ecr_cli.batch_get_image(
    registryId='576668000072',
    repositoryName='xtract-keyword',
    imageIds=[
        {
            #'imageDigest': 'sha256:fbe0716165a42bf01c368ef9af4ce39e7e49e0f8f6d7ef6add4ba0118e0cac1e',
            'imageTag': 'latest'
        },
    ],
    acceptedMediaTypes=[
        'application/vnd.docker.distribution.manifest.v1+json',
        'application/vnd.docker.distribution.manifest.v2+json',
        'application/vnd.oci.image.manifest.v1+json'

    ]
)

print(response)

# docker pull


