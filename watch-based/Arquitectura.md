## Flowchart: Custom Watch-Based Kubernetes Scheduler

```mermaid
flowchart TD

    A([Start Scheduler]) --> B[Load kubeconfig or in-cluster config]
    B --> C[Initialize CoreV1Api client]
    C --> D[Start Watch stream on all Pods]

    D --> E{Event has Pod object?}
    E -- No --> D
    E -- Yes --> F{Pod is Pending AND<br/>schedulerName matches?}

    F -- No --> D
    F -- Yes --> G[choose_node(): list nodes and count pods]
    G --> H[Select node with fewest running Pods]

    H --> I[bind_pod():<br/>Create V1Binding object]
    I --> J[Send binding to Kubernetes API]

    J --> K[Print binding result]
    K --> D
