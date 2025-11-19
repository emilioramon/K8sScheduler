import time
from kubernetes import client, config, watch
from datetime import datetime
import argparse

ROUND_ROBIN_INDEX = 0


# -----------------------------
# Configuración de cliente K8s
# -----------------------------
def load_client(kubeconfig=None):
    if kubeconfig:
        config.load_kube_config(kubeconfig)
    else:
        config.load_incluster_config()
    return client.CoreV1Api()

# -----------------------------
# Elegir nodo disponible
# -----------------------------
def choose_node(api, pod):
    global ROUND_ROBIN_INDEX
    nodes = api.list_node().items
    ready_workers = []

    for n in nodes:
        labels = n.metadata.labels or {}
        # Excluir control-plane
        if "node-role.kubernetes.io/control-plane" in labels:
            continue
        # Filtrar nodos Ready
        conditions = {c.type: c.status for c in n.status.conditions}
        if conditions.get("Ready") == "True":
            ready_workers.append(n.metadata.name)

    if not ready_workers:
        return None

    # Round-robin
    node = ready_workers[ROUND_ROBIN_INDEX % len(ready_workers)]
    ROUND_ROBIN_INDEX += 1
    return node

# -----------------------------
# Bindeo seguro de pods
# -----------------------------
def bind_pod(api, pod, node_name):
    if node_name is None:
        raise ValueError("Invalid value for `target`, must not be `None`")
    target = client.V1ObjectReference(api_version="v1", kind="Node", name=node_name)
    meta = client.V1ObjectMeta(name=pod.metadata.name, namespace=pod.metadata.namespace)
    body = client.V1Binding(metadata=meta, target=target)
    api.create_namespaced_binding(namespace=pod.metadata.namespace, body=body)

# -----------------------------
# Main scheduler
# -----------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scheduler-name", required=True, help="Nombre de tu scheduler")
    parser.add_argument("--kubeconfig", default=None, help="Ruta al kubeconfig (opcional)")
    args = parser.parse_args()

    api = load_client(args.kubeconfig)
    print(f"[watch-student] scheduler starting… name={args.scheduler_name}")

    w = watch.Watch()

    for event in w.stream(api.list_pod_for_all_namespaces, _request_timeout=60):
        pod = event['object']
        if pod is None or not hasattr(pod, 'spec'):
            continue

        # Solo pods pendientes asignados a este scheduler
        if pod.spec.node_name is None and pod.spec.scheduler_name == args.scheduler_name:
            try:
                node = choose_node(api, pod)
                if node is None:
                    print(f"[{datetime.now()}] No nodes Ready for pod {pod.metadata.name}, skipping")
                    continue
                bind_pod(api, pod, node)
                print(f"[{datetime.now()}] Bound {pod.metadata.namespace}/{pod.metadata.name} -> {node}")
            except Exception as e:
                print(f"[{datetime.now()}] error binding pod {pod.metadata.namespace}/{pod.metadata.name}: {e}")


if __name__ == "__main__":
    main()
