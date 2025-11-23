
import argparse, math 
from kubernetes import client, config, watch 
import time
import random
from functools import wraps
from kubernetes.client import V1Binding, V1ObjectMeta, V1ObjectReference


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
# Filtrar nodos por etiquetas
# -----------------------------
def filter_nodes_by_labels(api: client.CoreV1Api, required_labels: dict):
    # Retorna solo nodos que tienen todas las etiquetas con los valores correctos
    # Si no se requieren etiquetas, retorna todos los nodos
    # Ejemplo de uso - hacer scheduling solo en nodos de producción
    # required_labels = {"env": "prod", "tier": "backend"}
    # filtered_nodes = filter_nodes_by_labels(api, pod, required_labels)

    all_nodes = api.list_node().items
    filtered_nodes = []
    
    # Chequeo cada nodo por etiquetas requeridas
    for node in all_nodes:
        node_labels = node.metadata.labels or {}
        matches_all = True
        # Verificar que todas las etiquetas requeridas estén presentes y coincidan
        for key, value in required_labels.items():
            if key not in node_labels or node_labels[key] != value:
                matches_all = False
                break
        # Si todas las etiquetas coinciden, incluir nodo
        if matches_all:
            filtered_nodes.append(node)
    
    print(f"Label filtering: {len(all_nodes)} -> {len(filtered_nodes)} nodes")
    return filtered_nodes

# -----------------------------
# Filtrar nodos por tolerancia a taints
# -----------------------------
def node_tolerates_taints(node: client.V1Node, pod: client.V1Pod) -> bool:
    # Chequeo si el pod tolera todos los taints del nodo
    taints = node.spec.taints or []
    tolerations = pod.spec.tolerations or []
    
    if not taints:
        return True
    # Chequeo cada taint contra las tolerancias del pod
    for taint in taints:
        tolerated = False
        # Chequeo cada tolerancia
        for tol in tolerations:
            # Chequeo si la tolerancia coincide con el taint
            key_match = tol.key == taint.key
            effect_match = (tol.effect == taint.effect or tol.effect == "NoExecute" or  tol.effect is None)
            # Evaluar basado en operador
            if tol.operator == "Exists":
                if key_match and effect_match:
                    tolerated = True
                    break
            elif tol.operator == "Equal":
                if (key_match and effect_match and 
                    tol.value == taint.value):
                    tolerated = True
                    break
        # Si no se encontró tolerancia para este taint, el nodo no es tolerado
        if not tolerated:
            print(f"Node {node.metadata.name} rejected due to taint {taint.key}={taint.value}:{taint.effect}")
            return False
            
    return True

def filter_nodes_by_taints(nodes, pod: client.V1Pod):
    # Filtrar nodos basados en tolerancia a taints
    tolerable_nodes = []
    # Chequea cada nodo por tolerancia a taints
    for node in nodes:
        if node_tolerates_taints(node, pod):
            tolerable_nodes.append(node)
    
    print(f"Taint filtering: {len(nodes)} -> {len(tolerable_nodes)} nodes")
    return tolerable_nodes


# -----------------------------
# Funciones de retry con backoff exponencial
# -----------------------------
def exponential_backoff(retries=3, base_delay=1.0, factor=2.0):
    """
    Decorador para reintentos con backoff exponencial.
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            delay = base_delay
            for attempt in range(1, retries + 1):
                try:
                    return func(*args, **kwargs)
                except client.rest.ApiException as e:
                    print(f"[WARNING] Attempt {attempt} failed: {e}")
                    if attempt == retries:
                        raise
                    print(f"[INFO] Retrying in {delay} seconds...")
                    time.sleep(delay)
                    delay *= factor
        return wrapper
    return decorator

@exponential_backoff(retries=3, base_delay=1.0)
def bind_pod_with_retry(api: client.CoreV1Api, pod, target_name: str):
    """
    Hace binding de un pod a un nodo con reintentos y backoff exponencial.
    """
    if target_name is None:
        raise ValueError("bind_pod_with_retry() received None as target")

    print(f"[DEBUG] Binding pod {pod.metadata.name} to node {target_name}...")

    # Crear Binding
    binding = {
        "apiVersion": "v1",
        "kind": "Binding",
        "metadata": {"name": pod.metadata.name},
        "target": {
            "apiVersion": "v1",
            "kind": "Node",
            "name": target_name
        }
    }

    try:
        # Hacer el bind del pod al nodo
        api.create_namespaced_binding(
            namespace=pod.metadata.namespace,
            body=binding
        )
        print(f"[OK] Pod {pod.metadata.name} bound successfully to {target_name}")
        return True

    except client.rest.ApiException as e:
        #print(f"[ERROR] Failed binding pod {pod.metadata.name}: {e}")
        #raise
    
def calculate_spread_score(node, pods, pod_labels_to_spread):
    # Lista de pods corriendo en el nodo
    running_pods = [p for p in pods if p.spec.node_name == node.metadata.name]
    
    if not running_pods:
        return 100  # Si el nodo esta vacio, devuelve el puntaje maximo
    
    # Contar pods con etiquetas similares
    similar_pod_count = 0
    for running_pod in running_pods:
        running_labels = running_pod.metadata.labels or {}
        matches = True
        for key, value in pod_labels_to_spread.items():
            if running_labels.get(key) != value:
                matches = False
                break
        if matches:
            similar_pod_count += 1
    
    # Menos pods similares -> mejor puntaje
    spread_score = max(0, 100 - (similar_pod_count * 20))
    return spread_score

#Elegir nodo considerando política de dispersión
def choose_node_with_spread(api: client.CoreV1Api, pod, spread_labels=None):
    
    nodes = api.list_node().items
    pods = api.list_pod_for_all_namespaces().items
    
    if not nodes:
        raise RuntimeError("No nodes available")
    
    # Aplicar filtro de etiquetas si es necesario
    if spread_labels:
        nodes = filter_nodes_by_labels(api, spread_labels)
    # Elimina nodos que no son tolerados por el pod
    nodes = filter_nodes_by_taints(nodes, pod)
    
    if not nodes:
        raise RuntimeError("No nodes match filtering criteria")
    
    # Scoring de nodos basado en múltiples factores
    best_score = -1
    best_node = None
    
    for node in nodes:
        # Numero de pods en el nodo (carga)
        pod_count = sum(1 for p in pods if p.spec.node_name == node.metadata.name)
        #Penaliza nodos con mas pods
        load_score = max(0, 100 - (pod_count * 10))
        
        # Dispersión de pods
        spread_score = 50  # Puntaje base si no hay política de dispersión
        if spread_labels:
            spread_score = calculate_spread_score(node, pods, spread_labels)
        
        # Score conbinado (weighted)
        total_score = (load_score * 0.6) + (spread_score * 0.4)
        
        print(f"Node {node.metadata.name}: load_score={load_score}, spread_score={spread_score}, total={total_score:.1f}")
        
        if total_score >= best_score:
            best_score = total_score
            best_node = node.metadata.name
    
    if not best_node:
        raise RuntimeError("Failed to select a node")

    
    return best_node

# Selección de nodo mejorada
def choose_node_enhanced(api: client.CoreV1Api, pod) -> str:
    
    # Definir políticas de scheduling
    required_labels = {"env": "prod"}  
    spread_policy = {"app": pod.metadata.labels.get("app")} if pod.metadata.labels else None
    
    try:
        # Obtener y filtrar nodos
        all_nodes = api.list_node().items
        print(f"Total nodes available: {len(all_nodes)}")
        
        # Filtrado por etiquetas
        label_filtered = filter_nodes_by_labels(api, required_labels)
        
        if not label_filtered:
            raise RuntimeError("No nodes satisfy label requirements")
        
        # Filtrado de taints
        taint_filtered = filter_nodes_by_taints(label_filtered, pod)
        
        if not taint_filtered:
            raise RuntimeError("No nodes satisfy label and taint requirements")
        
        # Elegir nodo considerando política de dispersión
        chosen_node = choose_node_with_spread(api, pod, spread_policy)

        if chosen_node is None:
            raise RuntimeError("No node could be chosen after scoring")
        
        print(f"[DEBUG] Chosen node: {chosen_node}")
        
        return chosen_node
        
    except Exception as e:
        print(f"Node selection error: {e}")
        raise

def main_enhanced():
    """Enhanced scheduler main function"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--scheduler-name", default="enhanced-scheduler-e")
    parser.add_argument("--kubeconfig", default=None)
    args = parser.parse_args()

    api = load_client(args.kubeconfig)
    print(f"Enhanced scheduler starting...")

    w = watch.Watch()

    for event in w.stream(api.list_pod_for_all_namespaces, timeout_seconds=60):
        obj = event.get('object')
        if obj is None or not hasattr(obj, 'spec') or not hasattr(obj, 'metadata'):
                continue

        if (obj.status.phase == "Pending" and
             getattr(obj.spec, 'scheduler_name', None) == args.scheduler_name and
             getattr(obj.spec, 'node_name', None) is None):

             print(f"Scheduling pod: {obj.metadata.namespace}/{obj.metadata.name}")
             try:
                 node_name = choose_node_enhanced(api, obj)
                 print(f"[DEBUG] node_name resolved: {node_name}")
                 print(f"[DEBUG] pod.metadata.name: {obj.metadata.name}")
                 print(f"[DEBUG] node before bind: {node_name}")
                 if not node_name or not obj.metadata.name:
                     print(f"[ERROR] Cannot bind pod: node_name={node_name}, pod_name={obj.metadata.name}")
                     continue
                 
                 bind_pod_with_retry(api, obj, node_name)
                 print(f"Successfully scheduled {obj.metadata.name} -> {node_name}")

             except Exception as e:
                print(f"Failed to schedule pod {obj.metadata.name}: {e}")
             

if __name__ == "__main__":
    main_enhanced()
