import kubeApiHandler as kubeApi


def load_resource(namespace, category, callback):
    # Get resource
    return kubeApi.get_resource(namespace, category, callback)


def apply_resource(data, category, namespace, abort):
    if category == "configmap":
        return kubeApi.create_configmap(data, namespace)
    if category == "deployment":
        return kubeApi.create_deployment(data, namespace)
    abort(category)
