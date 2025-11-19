import argparse
import math 
import sys
from kubernetes import client, config, watch 

def node_tolerates_taints(node, pod):
    """Check if pod tolerates all node taints"""
    taints = node.spec.taints or []
    tolerations = pod.spec.tolerations or []
    
    if not taints:
        return True
        
    for taint in taints:
        tolerated = any(
            tol.key == taint.key and
            (tol.effect == taint.effect or tol.effect is None) and
            (tol.operator == "Exists" or tol.value == taint.value)
            for tol in tolerations
        )
        if not tolerated:
            return False
    return True

def load_client(kubeconfig=None):
    try:
        if kubeconfig:
            config.load_kube_config(kubeconfig)
        else:
            config.load_incluster_config()
        return client.CoreV1Api()
    except Exception as e:
        print(f"Error loading Kubernetes config: {e}")
        sys.exit(1)

def bind_pod(api: client.CoreV1Api, pod, node_name: str):
    try:
        target = client.V1ObjectReference(kind="Node", name=node_name)
        meta = client.V1ObjectMeta(name=pod.metadata.name)
        body = client.V1Binding(target=target, metadata=meta)
        api.create_namespaced_binding(pod.metadata.namespace, body)
        print(f"Successfully bound pod {pod.metadata.name} to node {node_name}")
    except Exception as e:
        print(f"Error binding pod: {e}")
        raise

def choose_node(api: client.CoreV1Api, pod) -> str:
    try:
        nodes = api.list_node().items
        if not nodes:
            raise RuntimeError("No nodes available")
        
        pods = api.list_pod_for_all_namespaces().items
        min_cnt = math.inf
        pick = None
        
        # Filter nodes that pod can tolerate
        tolerable_nodes = []
        for n in nodes:
            if node_tolerates_taints(n, pod):
                tolerable_nodes.append(n)
                print(f"Node {n.metadata.name} is tolerable for pod {pod.metadata.name}")
            else:
                print(f"Node {n.metadata.name} has intolerable taints for pod {pod.metadata.name}")
        
        if not tolerable_nodes:
            raise RuntimeError(f"No nodes with tolerable taints for pod {pod.metadata.name}")
        
        # Choose from tolerable nodes based on least loaded
        for n in tolerable_nodes:
            cnt = sum(1 for p in pods if p.spec.node_name == n.metadata.name)
            print(f"Node {n.metadata.name} has {cnt} pods and is tolerable")
            
            if cnt < min_cnt:
                min_cnt = cnt
                pick = n.metadata.name
                
        if pick is None:
            pick = tolerable_nodes[0].metadata.name
            
        print(f"Selected node: {pick} with {min_cnt} pods (taint-aware selection)")
        return pick
        
    except Exception as e:
        print(f"Error choosing node: {e}")
        raise

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scheduler-name", default="taint-aware-scheduler")
    parser.add_argument("--kubeconfig", default=None)     
    args = parser.parse_args() 

    try:
        api = load_client(args.kubeconfig)
        print(f"Taint-aware scheduler starting... name: {args.scheduler_name}")
        
        # Test connection and list nodes with taints
        nodes = api.list_node()
        print(f"Connected to cluster. Available nodes: {len(nodes.items)}")
        
        for node in nodes.items:
            taints = node.spec.taints or []
            taint_info = ", ".join([f"{t.key}={t.value}:{t.effect}" for t in taints]) if taints else "No taints"
            print(f"Node {node.metadata.name}: {taint_info}")
        
        w = watch.Watch()
        print("Starting to watch for pods...")
        
        for evt in w.stream(api.list_pod_for_all_namespaces, timeout_seconds=60):
            obj = evt['object']
            
            if obj is None or not hasattr(obj, 'spec'):
                continue
                
            # Check if pod is pending and has our scheduler name
            if (obj.status.phase == "Pending" and 
                hasattr(obj.spec, 'scheduler_name') and 
                obj.spec.scheduler_name == args.scheduler_name and
                obj.spec.node_name is None):
                
                print(f"Found pending pod: {obj.metadata.namespace}/{obj.metadata.name}")
                
                # Log pod tolerations
                tolerations = obj.spec.tolerations or []
                if tolerations:
                    tol_info = ", ".join([f"{tol.key}(op:{tol.operator})" for tol in tolerations])
                    print(f"Pod tolerations: {tol_info}")
                else:
                    print("Pod has no tolerations")
                
                try:
                    node = choose_node(api, obj)
                    bind_pod(api, obj, node)
                    print(f"Successfully scheduled {obj.metadata.name} -> {node}")
                except Exception as e:
                    print(f"Failed to schedule pod {obj.metadata.name}: {e}")
                    
    except Exception as e:
        print(f"Fatal error in scheduler: {e}")
        sys.exit(1)

if __name__ == "__main__":     
    main()