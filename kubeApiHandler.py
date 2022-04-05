from kubernetes import config, dynamic, client
from kubernetes.client import api_client

from flask import send_file
import os
import zipfile

# normal
TEST_MODE = 0

configmaps = {
    'pm-modbus': '_schema_ = "protocol/modbus-1.toml"\n\n[connections]\n[connections.conn1]\nconn_url = "tcp://192.168.1.44:502"\nconn_pool_count = 1\n[connections.conn2]\nconn_url = "tcp://192.168.1.211:502"\nconn_pool_count = 2\n\n[readers]\n[readers.r1]\nmode = "poll"\ninterval_ms = 1000\nconnection_name = "conn1"\ndpg_list = ["sensor1"]\n#option = "batch"\nout_topic = "modbus.conn1"\n[readers.r2]\nmode = "poll"\ninterval_ms = 1000\nconnection_name = "conn2"\ndpg_list = ["sensor1"]\n#option = "batch"\nout_topic = "modbus.conn2"\n[readers.r3]\nmode = "poll"\ninterval_ms = 1000\nconnection_name = "conn1"\ndpg_list = ["sensor1"]\n#option = "batch"\nout_topic = "modbus.conn3"\n[readers.r4]\nmode = "poll"\ninterval_ms = 1000\nconnection_name = "conn1"\ndpg_list = ["sensor1"]\n#option = "batch"\nout_topic = "modbus.conn4"\n[dpg_list]\n[dpg_list.sensor1]\n[dpg_list.sensor1.dp_list.dp1]\nregister_type = "holding"\naddress = 0\n[dpg_list.sensor1.dp_list.dp2]\ndata_type = "int32"\nregister_type = "holding"\naddress = 1\n',
    'sender-file2': '_schema_ = ["sender/file-writer-1.toml"]\n[queue]\ntopic = "process"\n[formatter]\n[file_writer]\nfilename = "$stdout$"\n',
    'sender-amqp': '_schema_ = ["sender/amqp-1.toml"]\n[amqp]\nhost="192.168.1.90"\nport=5672\nusername ="guest"\npassword="guest"\n[queue]\ntopic = "process2"\n[formatter]\n[file_writer]\nfilename = "$stdout$"\n',
    'sender-file': '_schema_ = ["sender/file-writer-1.toml"]\n[queue]\ntopic = "sender3"\n[formatter]\n[file_writer]\nfilename = "$stdout$"\n',
    'matched-pipe': '_schema_ = ["builtin/pipeline-1.toml", "builtin/processors-1.toml"]\nversion = "2"\n[processors]\n[processors.A]\ntype = "$builtin/processors/select-label-processor"\nlist = ["express"]\n[processors.B]\nin_topic = "modbus.conn1"\ntype = "$builtin/processors/script-processor"\nscript_file = "label.lua"\nfunction = "set_label"\n[processors.C]\ntype = "$builtin/processors/select-label-processor"\nlist = ["express"]\nout_topic = "process2"\n[processors.D]\ntype = "$builtin/processors/script-processor"\nscript_file = "print.lua"\nfunction = "print_all"        \nout_topic = "process" \n\n[[pipeline]]\nsource = "B"\ntarget = "A"\nout_port = "next"\n[[pipeline]]\nsource = "A"\ntarget = "C"\nout_port = "matched"\n[[pipeline]]\nsource = "C"\ntarget = "D"\nout_port = "unmatched"\n',
    'serial-pipe': '_schema_ = ["builtin/pipeline-1.toml", "builtin/processors-1.toml"]\nversion = "2"\n[processors]\n[processors.A]\ntype = "$builtin/processors/select-label-processor"\nlist = ["express"]\n[processors.B]\nin_topic = "modbus.conn2"\ntype = "$builtin/processors/script-processor"\nscript_file = "label.lua"\nfunction = "set_label"\n[processors.C]\ntype = "$builtin/processors/script-processor"\nscript_file = "print.lua"\nfunction = "print_express"\n[processors.D]\ntype = "$builtin/processors/script-processor"\nscript_file = "print.lua"\nfunction = "print_all"        \nout_topic = "sender3" \n\n[[pipeline]]\nsource = "B"\ntarget = "A"\nout_port = "next"\n[[pipeline]]\nsource = "A"\ntarget = "C"\nout_port = "all"\n[[pipeline]]\nsource = "C"\ntarget = "D"\nout_port = "next"\n',
    'test-pipe': '_schema_ = ["builtin/pipeline-1.toml", "builtin/processors-1.toml"]\nversion = "2"\n[processors]\n[processors.A]\ntype = "$builtin/processors/select-label-processor"\nlist = ["express"]\nin_topic = "modbus.conn4"\n[processors.B]\nin_topic = "modbus.conn3"\ntype = "$builtin/processors/script-processor"\nscript_file = "label.lua"\nfunction = "set_label"\n[processors.C]\ntype = "$builtin/processors/script-processor"\nscript_file = "print.lua"\nfunction = "print_express"\n[processors.D]\ntype = "$builtin/processors/script-processor"\nscript_file = "print.lua"\nfunction = "print_all"        \nout_topic = "sender3" \n\n[[pipeline]]\nsource = "B"\ntarget = "C"\nout_port = "all"\n[[pipeline]]\nsource = "A"\ntarget = "C"\nout_port = "all"\n[[pipeline]]\nsource = "C"\ntarget = "D"\nout_port = "next"\n'
}

deployments = {
    'pm-modbus': {'deployment': 'pm-modbus', 'status': 1},
    'matched-pipe': {'deployment': 'matched-pipe', 'status': None},
    'test-pipe': {'deployment': 'test-pipe', 'status': None},
    'serial-pipe': {'deployment': 'serial-pipe', 'status': 1},
    'sender-amqp': {'deployment': 'sender-amqp', 'status': 1}
}

customs = {
    'pm-modbus-2': {'category': 'protocol', 'image': '192.168.1.105/oncue/pm-modbus', 'tag': '2.0'},
    'pm-modbus-1': {'category': 'protocol', 'image': '192.168.1.105/oncue/pm-modbus', 'tag': '1.0'},
    'processing-1': {'category': 'processing', 'image': '192.168.1.105/oncue/processor', 'tag': '1.0'},
    'filewriter-1': {'category': 'processing', 'image': '192.168.1.105/oncue/sender-file-writer', 'tag': '1.0'},
    'amqp-1': {'category': 'processing', 'image': '192.168.1.105/oncue/sender-amqp', 'tag': '1.0'},
    'processing-1': {'category': 'processing', 'image': '192.168.1.105/oncue/processor', 'tag': '1.0'},
}

if TEST_MODE == 0:
    try:
        dynamic_client = dynamic.DynamicClient(
            api_client.ApiClient(configuration=config.load_kube_config())
        )
    except Exception as ec:
        raise ValueError("kubeApiHandler::Config data is not loaded")


def Zip(path):
    print(path)
    fantasy_zip = zipfile.ZipFile('./schema.zip', 'w')
    for folder, subfolders, files in os.walk(path):
        print(files)
        for file in files:
            if file.endswith('.zip'):
                fantasy_zip.write(os.path.join(folder, file),
                                  os.path.relpath(os.path.join(folder, file), path),
                                  compress_type=zipfile.ZIP_DEFLATED)

    fantasy_zip.close()


def create_configmap(data, namespace):
    if TEST_MODE == 1:
        for name in data:
            configmaps[name] = data[name]
            return

    api = dynamic_client.resources.get(api_version="v1", kind="ConfigMap")

    for name in data:
        configmap_manifest = {
            "kind": "ConfigMap",
            "apiVersion": "v1",
            "metadata": {
                "name": name,
            },
            "data": {
                "config.toml": data[name]
            },
        }
        try:
            api.patch(
                name=name, namespace=namespace, body=configmap_manifest
            )
            print("\n[INFO] configmap", name, "Patched\n")
            continue
        except Exception as ec:
            # Creating configmap
            api.create(body=configmap_manifest, namespace=namespace)
            print("\n[INFO] configmap", name, "Created\n")

    return 200


def create_deployment(data, namespace):
    if TEST_MODE == 1:
        print(data)
        return 200

    api = dynamic_client.resources.get(api_version="apps/v1", kind="Deployment")

    for name in data:
        deployment_file = {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": name,
                "namespace": namespace
            },
            "spec": {
                "replicas": 1,
                "selector": {
                    "matchLabels": {
                        "app": name,
                    }
                },
                "template": {
                    "metadata": {
                        "annotations": {
                            "sidecar.istio.io/inject": "false"
                        },
                        "labels": {
                            "app": name,
                        }
                    },
                    "spec": {
                        "containers": [
                            {
                                "name": name,
                                "image": data[name]["image"],
                                "volumeMounts": [
                                    {
                                        "name": "config-volume",
                                        "mountPath": "/var/lib/oncue/config"
                                    },
                                    {
                                        "name": "schema-volume",
                                        "mountPath": "/var/lib/oncue/schema"
                                    },
                                ]
                            }],
                        "volumes": [
                            {
                                "name": "config-volume",
                                "configMap": {
                                    "name": data[name]["configmap"]
                                }
                            },
                            {
                                "name": "schema-volume",
                                "hostPath": {
                                    "path": "/var/lib/oncue/schema",
                                    "type": "Directory"
                                }
                            },
                            {
                                "name": "actcode",
                                "secret": {"secretName": "actcode",
                                           "defaultMode": 400
                                           }
                            }
                        ]
                    }
                }
            }
        }

        try:
            api.patch(
                name=name, namespace=namespace, body=deployment_file
            )
            print("\n[INFO] Deployment", name, "Patched\n")
            continue
        except Exception as ec:
            # Creating Deployment
            api.create(body=deployment_file, namespace=namespace)
            print("\n[INFO] Deployment", name, "Created\n")

    return 200


def get_namespace():
    api = client.resources.get(api_version="v1", kind="Namespace")
    got = api.get(name=None).items
    api.get_namespaced_custom_object()
    namespace = []
    for ele in got:
        namespace.append(ele["metadata"]["name"])

    print("kubeApiHandler::get_namespace - loaded namespace: " + ','.join(namespace))

    return namespace


def get_resource(namespace, category, abort):
    if category == "configmap":
        got = get_configmap(namespace)
        if got is not None:
            print("kubeApiHandler::get_configmap - ", got)
            return got
    if category == "images":
        got = get_custom(namespace)
        if got is not None:
            print("kubeApiHandler::images - ", got)
            return got
    if category == "schema":
        got = get_schema()
        if got is not None:
            print("kubeApiHandler::schema - ", got)
            return got
    if category == "deployment":
        got = get_deployments(namespace)
        if got is not None:
            print("\nkubeApiHandler::get_deployments - ", got)
            return got
    if category == "deployManifest":
        got = get_deployManifest(namespace)
        if got is not None:
            print("\nkubeApiHandler::get_deployManifest - ", got)
            return got
    if category == "configManifest":
        got = get_configManifest(namespace)
        if got is not None:
            print("\nkubeApiHandler::get_configManifest - ", got)
            return got
    abort(category)


def get_schema():
    Zip("/var/lib/oncue/schema/archive")
    file_name = f"./schema.zip"
    return send_file(file_name,
                     attachment_filename='schema.zip',  # 다운받아지는 파일 이름.
                     as_attachment=True)


def get_configmap(target):
    if TEST_MODE == 1:
        return configmaps
    # Listing the configmaps in the `default` namespace
    api = dynamic_client.resources.get(api_version="v1", kind="ConfigMap")
    resources = api.get(name=None, namespace=target)
    names = []
    for ele in resources.items:
        if ele["data"]["config.toml"]:
            names.append(ele["metadata"]["name"])

    loaded = {}
    for ele in names:
        configmap = api.get(name=ele, namespace=target)
        data = configmap["data"]
        key = list(data.keys())[0]
        loaded.update({ele: data[key]})

    return loaded


def get_custom(target):
    if TEST_MODE == 1:
        return customs

    # Listing the configmaps in the `default` namespace
    custom_object_api = client.CustomObjectsApi()
    rst = custom_object_api.list_cluster_custom_object(
        group="oncue.sdplex.com", version="v1", plural="images"
    )

    response = {}

    for item in rst["items"]:
        meta = item["metadata"]
        name = meta["name"]
        namespace = meta["namespace"]
        if namespace == target:
            spec = item["spec"]
            response[name] = spec

        print("get_customresource - ", response)

    return response


def get_deployments(namespace):
    if TEST_MODE == 1:
        return deployments

    v1 = client.AppsV1Api()
    ret = v1.list_namespaced_deployment(namespace=namespace, pretty="pretty")
    response = {}

    for deployment in ret.items:
        name = deployment.metadata.name
        volumes = deployment.spec.template.spec.volumes

        configName = ""
        for element in volumes:
            if element.config_map != None:
                configName = element.config_map.name

        if configName == "":
            continue

        status = deployment.status.available_replicas
        response[name] = {
            "config": configName,
            "status": status
        }

    return response


def get_configManifest(target):
    configmap_manifest = {
        "kind": "ConfigMap",
        "apiVersion": "v1",
        "metadata": {
            "name": None,
            "namespace": target
        },
        "data": {
            "config.toml": None
        },
    }

    return configmap_manifest


def get_deployManifest(target):
    deployment_file = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": None,
            "namespace": target
        },
        "spec": {
            "replicas": 1,
            "selector": {
                "matchLabels": {
                    "app": None,
                }
            },
            "template": {
                "metadata": {
                    "annotations": {
                        "sidecar.istio.io/inject": "false"
                    },
                    "labels": {
                        "app": None,
                    }
                },
                "spec": {
                    "containers": [
                        {
                            "name": None,
                            "image": None,
                            "volumeMounts": [
                                {
                                    "name": "config-volume",
                                    "mountPath": "/var/lib/oncue/config"
                                },
                                {
                                    "name": "schema-volume",
                                    "mountPath": "/var/lib/oncue/schema"
                                },
                            ]
                        }],
                    "volumes": [
                        {
                            "name": "config-volume",
                            "configMap": {
                                "name": None
                            }
                        },
                        {
                            "name": "schema-volume",
                            "hostPath": {
                                "path": "/var/lib/oncue/schema",
                                "type": "Directory"
                            }
                        },
                        {
                            "name": "actcode",
                            "secret": {"secretName": "actcode",
                                       "defaultMode": 400
                                       }
                        }
                    ]
                }
            }
        }
    }

    return deployment_file
