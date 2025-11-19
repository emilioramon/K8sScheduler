## Scheduler Architecture & Flow - Enhanced Scheduler Architecture & Flow

```mermaid
flowchart TD

    %% ============================
    %% USER + POD CREATION
    %% ============================
    subgraph User
        A[Developer creates Pod<br/>schedulerName=enhanced-scheduler-e]
    end

    %% ============================
    %% API SERVER
    %% ============================
    subgraph APIServer
        S[(Kubernetes API Server)]
    end

    %% ============================
    %% SCHEDULER CORE
    %% ============================
    subgraph Scheduler["Enhanced Python Scheduler"]
        L[load_client]
        W[watch Pods<br/>Pending + schedulerName match]

        F1[filter_nodes_by_labels]
        F2[filter_nodes_by_taints]

        SP1[calculate_spread_score]
        CH[choose_node_with_spread]
        CH2[choose_node_enhanced]

        R[bind_pod_with_retry<br/>exponential backoff]
    end

    %% ============================
    %% CLUSTER NODES
    %% ============================
    subgraph ClusterNodes
        N1((Node 1))
        N2((Node 2))
        N3((Node 3))
    end

    %% ============================
    %% MAIN FLOW
    %% ============================

    A -->|Create Pod| S
    S -->|Watch stream events| W

    W -->|Pending + matches| CH2

    %% Node selection pipeline
    CH2 --> F1
    F1 --> F2
    F2 --> CH
    CH --> SP1
    SP1 --> CH
    CH -->|Best scoring node| R

    %% Binding
    R -->|POST /binding| S

    %% API assigns pod to node
    S --> N1
    S --> N2
    S --> N3

    %% Loop
    R --> W
