Because of the `build: .` directive and the corresponding `Dockerfile` in the `docker-compose.yaml` file, the dependencies from `requirements-airflow.txt` would be redownloaded into aiflow upon
```bash
docker compose restart
```