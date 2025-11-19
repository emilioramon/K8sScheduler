## Scheduler Architecture & Flow

```mermaid
flowchart TD
    %% === User / Pod Creation ===
    subgraph User
        A[Developer creates Pod with schedulerName=my-scheduler-w]
    end

    %% === Kubernetes API ===
    subgraph KubernetesAPI
        F[(API Server)]
    end

    %% === Scheduler ===
    subgraph Scheduler
        B[load_client - load kubeconfig or in-cluster]
        C[watch.Watch - stream Pod events]
        D{Pod Pending and schedulerName matches?}
        E[choose_node - select node with fewest pods]
        G[bind_pod - create V1Binding]
        H[Send binding to Kubernetes API]
        I[Print binding result]
    end

    %% === Cluster Nodes ===
    subgraph ClusterNodes
        N1((Node 1))
        N2((Node 2))
        N3((Node 3))
    end

    %% === Flow Connections ===
    A -->|Creates Pod| F
    F -->|Sends Pod events| C
    C --> D
    D -- No --> C
    D -- Yes --> E
    E --> G
    G --> H
    H --> I
    I --> C

    %% === API -> Nodes binding ===
    H --> N1
    H --> N2
    H --> N3
