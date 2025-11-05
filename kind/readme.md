# Cluster Kind para el planificador

Este entorno permite desplegar el planificador en un cluster local usando Kind.

## Pasos para levantar el entorno

1. Crear el cluster:
   ```bash
   kind create cluster --name sched-lab --config kind-config.yaml
