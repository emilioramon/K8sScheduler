```markdown
## Architecture Diagram

```mermaid
graph LR
    subgraph User
        A[Developer creates Pod with schedulerName=my-scheduler-w]
    end

    subgraph Scheduler
        B[load_client - load kubeconfig or in-cluster]
        C[watch.Watch - stream Pod events]
        D[choose_node - pick node with fewest Pods]
        E[bind_pod - create V1Binding]
    end

    subgraph KubernetesAPI
        F[(API Server)]
    end

    subgraph ClusterNodes
        N1((Node 1))
        N2((Node 2))
        N3((Node 3))
    end

    A -->|Creates Pod| F
    F -->|Sends Pod events| C

    C -->|Pending & scheduler matches| D
    D -->|Selected node| E
    E -->|POST /binding| F

    F --> N1
    F --> N2
    F --> N3
