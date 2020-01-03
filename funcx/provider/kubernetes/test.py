from kube import KubernetesProvider as k8s

p = k8s(image='location')

p.submit('test', image='new image')
