import argparse, math 
from kubernetes import client, config, watch 

#python scheduler.py --scheduler-name my-scheduler_w --kubeconfig ~/.kube/config

# TODO: load_client(kubeconfig) -> CoreV1Api 
#  - Use config.load_incluster_config() by default, else config.load_kube_config() 
def load_client(kubeconfig=None):
    if kubeconfig:
        config.load_kube_config(kubeconfig)
    else:
        config.load_incluster_config()
    return client.CoreV1Api()

# TODO: bind_pod(api, pod, node_name) 
#  - Create a V1Binding with metadata.name=pod.name and target.kind=Node,target.name=node_name 
#  - Call api.create_namespaced_binding(namespace, body) 
def bind_pod(api: client.CoreV1Api, pod, node_name: str):
    target = client.V1ObjectReference(kind="Node", name=node_name)
    meta = client.V1ObjectMeta(name=pod.metadata.name)
    body = client.V1Binding(target=target, metadata=meta)
    api.create_namespaced_binding(pod.metadata.namespace, body)

# TODO: choose_node(api, pod) -> str 
#  - List nodes and pick one based on a simple policy (fewest running pods) 
def choose_node(api: client.CoreV1Api, pod) -> str:
    nodes = api.list_node().items
    if not nodes:
        raise RuntimeError("No nodes available")
    pods = api.list_pod_for_all_namespaces().items
    min_cnt = math.inf
    pick = nodes[0].metadata.name
    for n in nodes:
        cnt = sum(1 for p in pods if p.spec.node_name == n.metadata.name)
        if cnt < min_cnt:
            min_cnt = cnt
            pick = n.metadata.name
    return pick

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scheduler-name", default="my-scheduler-w")
    parser.add_argument("--kubeconfig", default=None)     
    args = parser.parse_args() 

    api = load_client(args.kubeconfig) 

    print(f"[watch-student] scheduler starting… name= {args.scheduler_name}")     
    w = watch.Watch() 
    # Stream Pod events across all namespaces 
    for evt in w.stream(client.CoreV1Api().list_pod_for_all_namespaces, _request_timeout=60):
        obj = evt['object'] 
        if obj is None or not hasattr(obj, 'spec'): continue 
        # TODO: Only act on Pending pods that target our schedulerName 
        #  - if obj.spec.node_name is not set and obj.spec.scheduler_name == args.scheduler_name: 
        #        node = choose_node(api, obj) 
        #        bind_pod(api, obj, node) 
        #        print(...) 
        if obj.spec.node_name is None and obj.spec.scheduler_name == args.scheduler_name:
            try:
                api = load_client(args.kubeconfig)
                node = choose_node(api, obj)
                bind_pod(api, obj, node)
                print(f"Bound {obj.metadata.namespace}/{obj.metadata.name} -> {node}")
            except Exception as e:
                print("error:", e)

if __name__ == "__main__":     main() 